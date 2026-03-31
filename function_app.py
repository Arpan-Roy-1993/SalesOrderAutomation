import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func

logging.warning("function_app module loading...")
import re

try:
    from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
    logging.warning("azure.core.exceptions import OK")
except Exception as e:
    logging.exception("azure.core.exceptions import FAILED: %s", e)
    raise

try:
    from azure.data.tables import TableServiceClient
    logging.warning("azure.data.tables import OK")
except Exception as e:
    logging.exception("azure.data.tables import FAILED: %s", e)
    raise

try:
    from azure.identity import DefaultAzureCredential
    logging.warning("azure.identity import OK")
except Exception as e:
    logging.exception("azure.identity import FAILED: %s", e)
    raise

try:
    from processor import process_message_by_id
    logging.warning("processor import OK")
except Exception as e:
    logging.exception("processor import FAILED: %s", e)
    raise

try:
    from azure.data.tables import UpdateMode
    logging.warning("azure.data.tables import OK")
except Exception as e:
    logging.exception("azure.data.tables import FAILED: %s", e)
    raise



app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

logging.warning("FunctionApp created successfully")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_table_client():
    logging.warning("Entering _get_table_client")

    account_name = os.environ.get("AzureWebJobsStorage__accountName")
    mi_client_id = os.environ.get("AzureWebJobsStorage__clientId")
    table_name = os.environ.get("MESSAGE_DEDUPE_TABLE", "processedmessages")

    logging.warning(
        "Table settings resolved. account_name=%r mi_client_id=%r table_name=%r",
        account_name,
        mi_client_id,
        table_name,
    )

    if not account_name:
        raise ValueError("Missing AzureWebJobsStorage__accountName")

    account_url = f"https://{account_name}.table.core.windows.net"
    logging.warning("Table account URL: %s", account_url)

    credential = DefaultAzureCredential(
        managed_identity_client_id=mi_client_id
    )
    logging.warning("DefaultAzureCredential for table client created")

    service = TableServiceClient(endpoint=account_url, credential=credential)
    logging.warning("TableServiceClient created")

    table_client = service.get_table_client(table_name=table_name)
    logging.warning("Table client acquired")

    try:
        table_client.create_table()
        logging.warning("Table created or already available: %s", table_name)
    except Exception as e:
        logging.warning("table_client.create_table() non-fatal result: %s", e)

    return table_client



def _safe_row_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", value)

def _message_entity_keys(message_id: str):
    return "graph-webhook-message", _safe_row_key(message_id)


def _get_message_entity(message_id: str):
    logging.warning("Entering _get_message_entity for message_id=%s", message_id)
    table = _get_table_client()
    pk, rk = _message_entity_keys(message_id)

    try:
        entity = table.get_entity(partition_key=pk, row_key=rk)
        logging.warning(
            "Existing dedupe entity found. message_id=%s status=%s",
            message_id,
            entity.get("status"),
        )
        return entity
    except ResourceNotFoundError:
        logging.warning("No existing dedupe entity for message_id=%s", message_id)
        return None


def _try_claim_message(message_id: str) -> bool:
    logging.warning("Entering _try_claim_message for message_id=%s", message_id)
    table = _get_table_client()
    pk, rk = _message_entity_keys(message_id)

    entity = {
        "PartitionKey": pk,
        "RowKey": rk,
        "status": "processing",
        "createdAtUtc": _utc_now_iso(),
        "updatedAtUtc": _utc_now_iso(),
    }

    try:
        table.create_entity(entity=entity)
        logging.warning("Claimed message successfully: %s", message_id)
        return True
    except ResourceExistsError:
        logging.warning("Message already claimed or exists: %s", message_id)
        return False
    except Exception:
        logging.exception("Unexpected failure claiming message_id=%s", message_id)
        raise


def _mark_message_processed(message_id: str) -> None:
    logging.warning("Marking message as processed: %s", message_id)
    table = _get_table_client()
    pk, rk = _message_entity_keys(message_id)

    entity = {
        "PartitionKey": pk,
        "RowKey": rk,
        "status": "processed",
        "updatedAtUtc": _utc_now_iso(),
    }
  

    table.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
    logging.warning("Marked processed successfully: %s", message_id)


def _mark_message_failed(message_id: str, error_text: str) -> None:
    logging.warning("Marking message as failed: %s", message_id)
    table = _get_table_client()
    pk, rk = _message_entity_keys(message_id)

    entity = {
        "PartitionKey": pk,
        "RowKey": rk,
        "status": "failed",
        "updatedAtUtc": _utc_now_iso(),
        "lastError": error_text[:3000],
    }
    table.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
    logging.warning("Marked failed successfully: %s", message_id)


@app.route(route="graph_webhook", methods=["GET", "POST"])
def graph_webhook(req: func.HttpRequest) -> func.HttpResponse:
    logging.warning("=== graph_webhook HIT ===")
    logging.warning("Method=%s", req.method)

    validation_token = req.params.get("validationToken")
    logging.warning("validationToken present=%s", bool(validation_token))

    if validation_token:
        logging.warning("Returning validationToken to Microsoft Graph")
        return func.HttpResponse(
            body=validation_token,
            status_code=200,
            mimetype="text/plain",
        )

    try:
        logging.warning("About to parse request JSON")
        body = req.get_json()
        logging.warning("Request JSON parsed successfully")
        logging.info("Request JSON body: %s", json.dumps(body))
    except ValueError:
        logging.exception("Invalid JSON payload")
        return func.HttpResponse("Invalid JSON", status_code=400)
    except Exception:
        logging.exception("Unexpected failure parsing request JSON")
        return func.HttpResponse("JSON parse failure", status_code=500)

    notifications = body.get("value", [])
    expected_client_state = os.environ.get("GRAPH_CLIENT_STATE")

    logging.warning("Notification count=%s", len(notifications))
    logging.warning("Expected client state=%r", expected_client_state)

    processed = 0
    skipped = 0
    failed = 0

    for idx, notification in enumerate(notifications, start=1):
        try:
            logging.warning("Handling notification %s/%s", idx, len(notifications))
            logging.info("Notification payload: %s", json.dumps(notification))

            client_state = notification.get("clientState")
            resource_data = notification.get("resourceData", {}) or {}
            message_id = resource_data.get("id")

            logging.warning(
                "Notification details: clientState=%r message_id=%r",
                client_state,
                message_id,
            )

            if expected_client_state and client_state != expected_client_state:
                logging.error(
                    "Skipping notification due to clientState mismatch. expected=%s actual=%s",
                    expected_client_state,
                    client_state,
                )
                skipped += 1
                continue

            if not message_id:
                logging.error("Skipping notification with no resourceData.id")
                skipped += 1
                continue

            logging.warning("About to check existing dedupe entity for %s", message_id)
            existing = _get_message_entity(message_id)

            if existing and existing.get("status") == "processed":
                logging.warning("Skipping duplicate already-processed message_id=%s", message_id)
                skipped += 1
                continue

            logging.warning("About to claim message_id=%s", message_id)
            if not _try_claim_message(message_id):
                logging.warning("Skipping duplicate in-flight or existing message_id=%s", message_id)
                skipped += 1
                continue

            logging.warning("About to call process_message_by_id for %s", message_id)
            result = process_message_by_id(message_id)
            logging.warning("process_message_by_id returned result=%r for %s", result, message_id)

            _mark_message_processed(message_id)
            processed += 1
            logging.warning("Finished processing message_id=%s", message_id)

        except Exception as exc:
            logging.exception("Notification handling failed for message_id=%s", message_id if 'message_id' in locals() else None)
            try:
                if 'message_id' in locals() and message_id:
                    _mark_message_failed(message_id, str(exc))
            except Exception:
                logging.exception("Failed while marking message failure for message_id=%s", message_id if 'message_id' in locals() else None)
            failed += 1

    response_text = f"Accepted. processed={processed}, skipped={skipped}, failed={failed}"
    logging.warning("Returning response: %s", response_text)

    return func.HttpResponse(
        body=response_text,
        status_code=202,
    )