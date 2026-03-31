import base64
import csv
import io
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from azure.data.tables import UpdateMode

import requests

logging.warning("processor module loading...")

try:
    from azure.ai.documentintelligence import DocumentIntelligenceClient
    logging.warning("DocumentIntelligenceClient import OK")
except Exception as e:
    logging.exception("DocumentIntelligenceClient import FAILED: %s", e)
    raise

try:
    from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
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
    from azure.identity import ClientSecretCredential, DefaultAzureCredential, AzureAuthorityHosts
    logging.warning("azure.identity imports OK")
except Exception as e:
    logging.exception("azure.identity imports FAILED: %s", e)
    raise

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    logging.warning("DataLakeServiceClient import OK")
except Exception as e:
    logging.exception("DataLakeServiceClient import FAILED: %s", e)
    raise

GRAPH_SCOPE = "https://graph.microsoft.com/.default"

logging.warning("=== PROCESSOR LOGGING BUILD ACTIVE ===")


@dataclass
class Settings:
    tenant_id: str
    client_id: str
    client_secret: str
    mailbox_user: str
    doc_intel_endpoint: str
    doc_intel_model: str
    workspace_name: str
    lakehouse_name: str
    onelake_account_url: str
    output_root: str
    dedupe_table: str
    storage_account_name: str
    storage_mi_client_id: str


def load_settings() -> Settings:
    settings = Settings(
        tenant_id=os.getenv("AZURE_TENANT_ID", "").strip(),
        client_id=os.getenv("AZURE_CLIENT_ID", "").strip(),
        client_secret=os.getenv("AZURE_CLIENT_SECRET", "").strip(),
        mailbox_user=os.getenv("MAILBOX_USER", "").strip(),
        doc_intel_endpoint=os.getenv("DOC_INTEL_ENDPOINT", "").strip(),
        doc_intel_model=os.getenv("DOC_INTEL_MODEL", "prebuilt-layout").strip(),
        workspace_name=os.getenv("FABRIC_WORKSPACE_NAME", "").strip(),
        lakehouse_name=os.getenv("FABRIC_LAKEHOUSE_NAME", "").strip(),
        onelake_account_url=os.getenv(
            "ONELAKE_ACCOUNT_URL",
            "https://onelake.dfs.fabric.microsoft.com",
        ).strip(),
        output_root=os.getenv("SALES_ORDERS_ROOT", "Files/pdf_extracts").strip(),
        dedupe_table=os.getenv("MESSAGE_DEDUPE_TABLE", "processedmessages").strip(),
        storage_account_name=os.getenv("AzureWebJobsStorage__accountName", "").strip(),
        storage_mi_client_id=os.getenv("AzureWebJobsStorage__clientId", "").strip(),
    )

    logging.warning(
        "Settings loaded. tenant_id_set=%s client_id_set=%s client_secret_set=%s mailbox_user=%r "
        "doc_intel_endpoint=%r doc_intel_model=%r workspace_name=%r lakehouse_name=%r "
        "onelake_account_url=%r output_root=%r dedupe_table=%r storage_account_name=%r storage_mi_client_id=%r",
        bool(settings.tenant_id),
        bool(settings.client_id),
        bool(settings.client_secret),
        settings.mailbox_user,
        settings.doc_intel_endpoint,
        settings.doc_intel_model,
        settings.workspace_name,
        settings.lakehouse_name,
        settings.onelake_account_url,
        settings.output_root,
        settings.dedupe_table,
        settings.storage_account_name,
        settings.storage_mi_client_id,
    )
    return settings


def get_credential():
    tenant_id = os.getenv("AZURE_TENANT_ID", "").strip()
    client_id = os.getenv("AZURE_CLIENT_ID", "").strip()
    client_secret = os.getenv("AZURE_CLIENT_SECRET", "").strip()

    logging.warning(
        "get_credential called. tenant_id_set=%s client_id_set=%s client_secret_set=%s",
        bool(tenant_id),
        bool(client_id),
        bool(client_secret),
    )

    if tenant_id and client_id and client_secret:
        logging.warning("Using ClientSecretCredential")
        logging.warning("AZURE_TENANT_ID runtime = %r", tenant_id)
        logging.warning("AZURE_CLIENT_ID runtime = %r", client_id)
        logging.warning("AZURE_AUTHORITY_HOST runtime = %r", os.getenv("AZURE_AUTHORITY_HOST"))

        return ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            authority=AzureAuthorityHosts.AZURE_PUBLIC_CLOUD,
        )

    logging.warning("Falling back to DefaultAzureCredential")
    return DefaultAzureCredential(exclude_interactive_browser_credential=True)


def get_table_credential(settings: Settings):
    logging.warning(
        "Creating table credential with managed_identity_client_id=%r",
        settings.storage_mi_client_id or None,
    )
    return DefaultAzureCredential(
        managed_identity_client_id=settings.storage_mi_client_id or None
    )


def get_access_token(scope: str) -> str:
    logging.warning("Attempting to get access token for scope=%s", scope)
    credential = get_credential()
    token = credential.get_token(scope)
    logging.warning("Access token acquired successfully for scope=%s", scope)
    return token.token


def graph_headers() -> Dict[str, str]:
    logging.warning("Building Graph headers")
    return {
        "Authorization": f"Bearer {get_access_token(GRAPH_SCOPE)}",
        "Accept": "application/json",
    }


def safe_raise_for_status(response: requests.Response) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError:
        logging.error("HTTP %s response body: %s", response.status_code, response.text)
        raise


def get_message(settings: Settings, message_id: str) -> Optional[Dict[str, Any]]:
    logging.warning("Fetching Graph message for message_id=%s", message_id)

    url = (
        f"https://graph.microsoft.com/v1.0/users/{settings.mailbox_user}/messages/{message_id}"
        "?$select=id,subject,receivedDateTime,from,hasAttachments,internetMessageId,parentFolderId"
    )
    logging.warning("Graph get_message URL=%s", url)

    response = requests.get(url, headers=graph_headers(), timeout=60)
    logging.warning("Graph get_message status_code=%s", response.status_code)

    if response.status_code == 404:
        logging.warning("Graph message not found for message_id=%s", message_id)
        return None

    safe_raise_for_status(response)
    message = response.json()

    logging.warning(
        "Graph message fetched successfully. subject=%s hasAttachments=%s internetMessageId=%s",
        message.get("subject"),
        message.get("hasAttachments"),
        message.get("internetMessageId"),
    )
    return message


def list_attachments(settings: Settings, message_id: str) -> List[Dict[str, Any]]:
    logging.warning("Listing attachments for message_id=%s", message_id)

    url = (
        f"https://graph.microsoft.com/v1.0/users/{settings.mailbox_user}/messages/{message_id}/attachments"
        "?$select=id,name,contentType,size"
    )
    logging.warning("Graph list_attachments URL=%s", url)

    response = requests.get(url, headers=graph_headers(), timeout=60)
    logging.warning("Graph list_attachments status_code=%s", response.status_code)

    if response.status_code == 404:
        logging.warning("Message disappeared before attachments could be listed. message_id=%s", message_id)
        return []

    safe_raise_for_status(response)
    items = response.json().get("value", [])
    logging.warning("Found %s attachment(s) from Graph for message_id=%s", len(items), message_id)
    return items


def download_attachment(settings: Settings, message_id: str, attachment_id: str) -> bytes:
    logging.warning(
        "Downloading attachment attachment_id=%s for message_id=%s",
        attachment_id,
        message_id,
    )

    url = (
        f"https://graph.microsoft.com/v1.0/users/{settings.mailbox_user}/messages/"
        f"{message_id}/attachments/{attachment_id}"
    )
    logging.warning("Graph download_attachment URL=%s", url)

    response = requests.get(url, headers=graph_headers(), timeout=60)
    logging.warning("Graph download_attachment status_code=%s", response.status_code)
    safe_raise_for_status(response)

    payload = response.json()
    content_b64 = payload.get("contentBytes")
    if not content_b64:
        raise ValueError(f"Attachment {attachment_id} did not include contentBytes")

    data = base64.b64decode(content_b64)
    logging.warning("Attachment download complete. bytes=%s", len(data))
    return data


def get_document_client(settings: Settings) -> DocumentIntelligenceClient:
    logging.warning("Creating Document Intelligence client for endpoint=%s", settings.doc_intel_endpoint)
    return DocumentIntelligenceClient(
        endpoint=settings.doc_intel_endpoint,
        credential=get_credential(),
    )


def analyze_document(settings: Settings, filename: str, file_bytes: bytes) -> Dict[str, Any]:
    logging.warning("Starting Document Intelligence analysis for filename=%s size=%s", filename, len(file_bytes))
    client = get_document_client(settings)

    poller = client.begin_analyze_document(
        model_id=settings.doc_intel_model,
        body=io.BytesIO(file_bytes),
        content_type="application/octet-stream",
    )
    logging.warning("Document Intelligence poller started for filename=%s", filename)

    result = poller.result()
    logging.warning("Document Intelligence poller completed for filename=%s", filename)

    result_dict = result.as_dict() if hasattr(result, "as_dict") else json.loads(json.dumps(result, default=str))
    logging.warning(
        "Document Intelligence result received for filename=%s tables=%s",
        filename,
        len(result_dict.get("tables", []) or []),
    )
    return result_dict


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def sanitize_path_part(value: str) -> str:
    value = value or "unknown"
    value = value.replace("/", "_").replace("\\", "_").replace(":", "_")
    value = value.replace("<", "_").replace(">", "_").replace("@", "_at_")
    return re.sub(r"[^A-Za-z0-9._=-]", "_", value)


def build_output_directory(settings: Settings, stable_id: str) -> str:
    stable = sanitize_path_part(stable_id)
    output_dir = f"{settings.output_root}/{stable}"
    logging.warning("Output directory resolved: %s", output_dir)
    return output_dir


def get_onelake_service_client(settings: Settings) -> DataLakeServiceClient:
    logging.warning("Creating OneLake DataLakeServiceClient account_url=%s", settings.onelake_account_url)
    return DataLakeServiceClient(
        account_url=settings.onelake_account_url,
        credential=get_credential(),
    )


def upload_bytes_to_onelake(
    settings: Settings,
    directory: str,
    file_name: str,
    data: bytes,
) -> str:
    logging.warning("Uploading file to OneLake. directory=%s file_name=%s bytes=%s", directory, file_name, len(data))

    service_client = get_onelake_service_client(settings)
    file_system_client = service_client.get_file_system_client(file_system=settings.workspace_name)
    logging.warning("OneLake file system client acquired for workspace=%s", settings.workspace_name)

    directory_path = f"{settings.lakehouse_name}.Lakehouse/{directory}"
    logging.warning("OneLake directory path=%s", directory_path)

    directory_client = file_system_client.get_directory_client(directory_path)
    try:
        directory_client.create_directory()
        logging.warning("Directory created: %s", directory_path)
    except Exception as e:
        logging.warning("Directory create non-fatal result for %s: %s", directory_path, e)

    full_file_path = f"{settings.lakehouse_name}.Lakehouse/{directory}/{file_name}"
    logging.warning("OneLake full file path=%s", full_file_path)

    file_client = file_system_client.get_file_client(full_file_path)

    try:
        file_client.delete_file()
        logging.warning("Deleted existing file before rewrite: %s", full_file_path)
    except Exception as e:
        logging.warning("Delete existing file non-fatal result for %s: %s", full_file_path, e)

    file_client.create_file()
    logging.warning("Created file: %s", full_file_path)

    file_client.append_data(data=data, offset=0, length=len(data))
    logging.warning("Append complete for file: %s", full_file_path)

    file_client.flush_data(len(data))
    logging.warning("Flush complete for file: %s", full_file_path)

    output_url = (
        f"{settings.onelake_account_url}/{settings.workspace_name}/"
        f"{settings.lakehouse_name}.Lakehouse/{directory}/{file_name}"
    )
    logging.warning("OneLake upload complete: %s", output_url)
    return output_url


def get_table_client(settings: Settings):
    logging.warning("Creating table client for storage_account_name=%r", settings.storage_account_name)

    if not settings.storage_account_name:
        raise ValueError("Missing AzureWebJobsStorage__accountName")

    account_url = f"https://{settings.storage_account_name}.table.core.windows.net"
    logging.warning("Table account URL=%s", account_url)

    service = TableServiceClient(endpoint=account_url, credential=get_table_credential(settings))
    logging.warning("TableServiceClient created in processor")

    table = service.get_table_client(table_name=settings.dedupe_table)
    logging.warning("Table client acquired in processor for table=%s", settings.dedupe_table)

    try:
        table.create_table()
        logging.warning("Table created or already available in processor: %s", settings.dedupe_table)
    except Exception as e:
        logging.warning("table.create_table() non-fatal result in processor: %s", e)

    return table


def attachment_row_key(message_id: str, attachment_name: str) -> str:
    base = f"{message_id}::{attachment_name}"
    rk = sanitize_path_part(base)[:1024]
    logging.warning("Attachment row key generated. attachment_name=%s row_key=%s", attachment_name, rk)
    return rk


def attachment_already_processed(settings: Settings, message_id: str, attachment_name: str) -> bool:
    logging.warning(
        "Checking attachment dedupe. message_id=%s attachment_name=%s",
        message_id,
        attachment_name,
    )
    table = get_table_client(settings)
    pk = "graph-webhook-attachment"
    rk = attachment_row_key(message_id, attachment_name)

    try:
        entity = table.get_entity(partition_key=pk, row_key=rk)
        status = entity.get("status")
        logging.warning(
            "Attachment dedupe entity found. message_id=%s attachment_name=%s status=%s",
            message_id,
            attachment_name,
            status,
        )
        return status == "processed"
    except ResourceNotFoundError:
        logging.warning(
            "No attachment dedupe entity found. message_id=%s attachment_name=%s",
            message_id,
            attachment_name,
        )
        return False


def mark_attachment_processed(settings: Settings, message_id: str, attachment_name: str, output_dir: str) -> None:
    logging.warning(
        "Marking attachment processed. message_id=%s attachment_name=%s output_dir=%s",
        message_id,
        attachment_name,
        output_dir,
    )
    table = get_table_client(settings)
    entity = {
        "PartitionKey": "graph-webhook-attachment",
        "RowKey": attachment_row_key(message_id, attachment_name),
        "status": "processed",
        "messageId": message_id,
        "attachmentName": attachment_name,
        "outputDir": output_dir,
    }
    table.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
    logging.warning("Attachment marked processed successfully")


def table_to_matrix(table: Dict[str, Any]) -> List[List[str]]:
    rows = table.get("row_count", 0)
    cols = table.get("column_count", 0)
    matrix = [["" for _ in range(cols)] for _ in range(rows)]

    for cell in table.get("cells", []):
        r = cell.get("row_index", 0)
        c = cell.get("column_index", 0)
        text = normalize_text(cell.get("content", ""))
        if r < rows and c < cols:
            matrix[r][c] = text

    return matrix


def clean_header_name(text: str, fallback_index: int) -> str:
    cleaned = normalize_text(text)
    if not cleaned:
        return f"column_{fallback_index}"
    cleaned = re.sub(r"[^A-Za-z0-9]+", "_", cleaned.lower()).strip("_")
    return cleaned or f"column_{fallback_index}"


def first_row_looks_like_header(row: List[str]) -> bool:
    if not row:
        return False

    non_empty = [normalize_text(x) for x in row if normalize_text(x)]
    if not non_empty:
        return False

    numeric_like = 0
    for value in non_empty:
        candidate = value.replace(",", "").replace("$", "")
        if re.fullmatch(r"-?\d+(\.\d+)?", candidate):
            numeric_like += 1

    return numeric_like < len(non_empty)


def extract_table_rows(
    result: Dict[str, Any],
    message: Dict[str, Any],
    attachment_name: str,
) -> List[Dict[str, str]]:
    tables = result.get("tables", []) or []

    message_subject = normalize_text(message.get("subject"))
    message_received = normalize_text(message.get("receivedDateTime"))
    internet_message_id = normalize_text(message.get("internetMessageId"))

    all_rows: List[Dict[str, str]] = []

    for table_idx, table in enumerate(tables, start=1):
        matrix = table_to_matrix(table)
        if not matrix:
            continue

        header_candidate = matrix[0]
        has_header = first_row_looks_like_header(header_candidate)

        if has_header:
            headers = [clean_header_name(cell, idx + 1) for idx, cell in enumerate(header_candidate)]
            data_rows = matrix[1:]
        else:
            headers = [f"column_{i+1}" for i in range(len(header_candidate))]
            data_rows = matrix

        for row_idx, row in enumerate(data_rows, start=1):
            cleaned_row = [normalize_text(x) for x in row]
            if not any(cleaned_row):
                continue

            record: Dict[str, str] = {
                "message_subject": message_subject,
                "message_received_datetime": message_received,
                "internet_message_id": internet_message_id,
                "attachment_name": attachment_name,
                "table_index": str(table_idx),
                "row_index_in_table": str(row_idx),
            }

            for col_idx, header in enumerate(headers):
                record[header] = cleaned_row[col_idx] if col_idx < len(cleaned_row) else ""

            all_rows.append(record)

    logging.warning("extract_table_rows produced row_count=%s for attachment=%s", len(all_rows), attachment_name)
    return all_rows


def extract_grouped_line_rows(
    result: Dict[str, Any],
    message: Dict[str, Any],
    attachment_name: str,
) -> List[Dict[str, str]]:
    content = result.get("content", "") or ""
    raw_lines = [normalize_text(line) for line in content.splitlines() if normalize_text(line)]

    message_subject = normalize_text(message.get("subject"))
    message_received = normalize_text(message.get("receivedDateTime"))
    internet_message_id = normalize_text(message.get("internetMessageId"))

    rows: List[Dict[str, str]] = []
    buffer: List[str] = []

    for line in raw_lines:
        buffer.append(line)

        if re.fullmatch(r"-?\d+(\.\d+)?", line.replace(",", "").replace("$", "")):
            if len(buffer) >= 4:
                rows.append(
                    {
                        "message_subject": message_subject,
                        "message_received_datetime": message_received,
                        "internet_message_id": internet_message_id,
                        "attachment_name": attachment_name,
                        "table_index": "0",
                        "row_index_in_table": str(len(rows) + 1),
                        "col_1": buffer[-4],
                        "col_2": buffer[-3],
                        "col_3": buffer[-2],
                        "col_4": buffer[-1],
                    }
                )
                buffer = []

    if not rows:
        for idx, line in enumerate(raw_lines, start=1):
            rows.append(
                {
                    "message_subject": message_subject,
                    "message_received_datetime": message_received,
                    "internet_message_id": internet_message_id,
                    "attachment_name": attachment_name,
                    "table_index": "0",
                    "row_index_in_table": str(idx),
                    "line_text": line,
                }
            )

    logging.warning("extract_grouped_line_rows produced row_count=%s for attachment=%s", len(rows), attachment_name)
    return rows


def rows_to_csv_bytes(rows: List[Dict[str, str]]) -> bytes:
    if not rows:
        headers = [
            "message_subject",
            "message_received_datetime",
            "internet_message_id",
            "attachment_name",
            "table_index",
            "row_index_in_table",
            "line_text",
        ]
    else:
        headers: List[str] = []
        for row in rows:
            for key in row.keys():
                if key not in headers:
                    headers.append(key)

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    data = buffer.getvalue().encode("utf-8")
    logging.warning("rows_to_csv_bytes produced bytes=%s", len(data))
    return data


def metadata_to_csv_bytes(message: Dict[str, Any], attachment_name: str, row_count: int, table_count: int) -> bytes:
    row = {
        "message_subject": normalize_text(message.get("subject")),
        "message_received_datetime": normalize_text(message.get("receivedDateTime")),
        "internet_message_id": normalize_text(message.get("internetMessageId")),
        "attachment_name": attachment_name,
        "table_count": str(table_count),
        "row_count": str(row_count),
    }

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(row.keys()), extrasaction="ignore")
    writer.writeheader()
    writer.writerow(row)
    data = buffer.getvalue().encode("utf-8")
    logging.warning("metadata_to_csv_bytes produced bytes=%s for attachment=%s", len(data), attachment_name)
    return data


def process_message_by_id(message_id: str) -> str:
    logging.warning("=== RUNNING GENERIC PDF PROCESSOR WITH LOGGING + DEDUPE ===")
    logging.warning("processor started for message_id=%s", message_id)

    settings = load_settings()

    try:
        logging.warning("About to fetch Graph message")
        message = get_message(settings, message_id)
        if not message:
            logging.warning("Skipping because Graph message no longer exists. message_id=%s", message_id)
            return "skipped"

        if not bool(message.get("hasAttachments")):
            logging.warning("Skipping message because it has no attachments. message_id=%s", message_id)
            return "skipped"

        logging.warning("About to list attachments")
        attachments = list_attachments(settings, message_id)
        logging.warning("Found %s attachment(s) for message %s", len(attachments), message_id)

        if not attachments:
            return "skipped"

        stable_id = message.get("internetMessageId") or message.get("id") or message_id
        output_dir = build_output_directory(settings, stable_id)

        any_processed = False
        any_skipped = False

        for attachment in attachments:
            name = attachment.get("name", "")
            odata_type = attachment.get("@odata.type", "")
            content_type = attachment.get("contentType", "")

            logging.warning(
                "Evaluating attachment name=%s contentType=%s odataType=%s",
                name,
                content_type,
                odata_type,
            )

            if odata_type and odata_type != "#microsoft.graph.fileAttachment":
                logging.warning("Skipping non-file attachment: %s", name)
                continue

            if not name.lower().endswith(".pdf"):
                logging.warning("Skipping non-PDF attachment: %s", name)
                continue

            logging.warning("About to check attachment dedupe for attachment=%s", name)
            if attachment_already_processed(settings, message_id, name):
                logging.warning(
                    "Skipping duplicate attachment already processed. message_id=%s attachment=%s",
                    message_id,
                    name,
                )
                any_skipped = True
                continue

            attachment_id = attachment.get("id")
            if not attachment_id:
                logging.warning("Skipping attachment with missing id: %s", name)
                continue

            logging.warning("About to download attachment=%s", name)
            file_bytes = download_attachment(settings, message_id, attachment_id)
            logging.warning("Attachment downloaded: %s", name)

            logging.warning("About to analyze document for attachment=%s", name)
            result_dict = analyze_document(settings, name, file_bytes)

            table_rows = extract_table_rows(result_dict, message, name)
            table_count = len(result_dict.get("tables", []) or [])
            logging.warning(
                "Post-analysis summary for attachment=%s table_count=%s structured_row_count=%s",
                name,
                table_count,
                len(table_rows),
            )

            if table_rows:
                logging.warning(
                    "Structured table extraction succeeded. table_count=%s row_count=%s",
                    table_count,
                    len(table_rows),
                )
                rows = table_rows
                used_fallback = False
            else:
                logging.warning("No usable tables found. Falling back to grouped line extraction for %s", name)
                rows = extract_grouped_line_rows(result_dict, message, name)
                used_fallback = True
                logging.warning("Fallback grouped-line extraction row_count=%s", len(rows))

            file_prefix = sanitize_path_part(name.rsplit(".", 1)[0])
            logging.warning("File prefix for attachment=%s is %s", name, file_prefix)

            metadata_csv_bytes = metadata_to_csv_bytes(message, name, len(rows), table_count)
            rows_csv_bytes = rows_to_csv_bytes(rows)
            raw_json_bytes = json.dumps(result_dict, indent=2).encode("utf-8")
            message_json_bytes = json.dumps(message, indent=2).encode("utf-8")
            parsed_json_bytes = json.dumps(
                {
                    "attachment_name": name,
                    "table_count": table_count,
                    "row_count": len(rows),
                    "used_fallback_grouped_line_extraction": used_fallback,
                    "rows": rows,
                },
                indent=2,
            ).encode("utf-8")

            logging.warning("About to upload metadata CSV for attachment=%s", name)
            upload_bytes_to_onelake(
                settings=settings,
                directory=output_dir,
                file_name=f"{file_prefix}_document_metadata.csv",
                data=metadata_csv_bytes,
            )

            logging.warning("About to upload rows CSV for attachment=%s", name)
            upload_bytes_to_onelake(
                settings=settings,
                directory=output_dir,
                file_name=f"{file_prefix}_document_rows.csv",
                data=rows_csv_bytes,
            )

            logging.warning("About to upload raw results JSON for attachment=%s", name)
            upload_bytes_to_onelake(
                settings=settings,
                directory=output_dir,
                file_name=f"{file_prefix}_raw_results.json",
                data=raw_json_bytes,
            )

            logging.warning("About to upload message metadata JSON for attachment=%s", name)
            upload_bytes_to_onelake(
                settings=settings,
                directory=output_dir,
                file_name=f"{file_prefix}_message_metadata.json",
                data=message_json_bytes,
            )

            logging.warning("About to upload parsed rows JSON for attachment=%s", name)
            upload_bytes_to_onelake(
                settings=settings,
                directory=output_dir,
                file_name=f"{file_prefix}_parsed_rows.json",
                data=parsed_json_bytes,
            )

            logging.warning("About to mark attachment processed for attachment=%s", name)
            mark_attachment_processed(settings, message_id, name, output_dir)
            any_processed = True
            logging.warning("Completed attachment=%s successfully", name)

        if any_processed:
            logging.warning("process_message_by_id completed with result=processed for message_id=%s", message_id)
            return "processed"
        if any_skipped:
            logging.warning("process_message_by_id completed with result=skipped for message_id=%s", message_id)
            return "skipped"

        logging.warning("process_message_by_id completed with no processed attachments; returning skipped")
        return "skipped"

    except HttpResponseError:
        logging.exception("Azure SDK error while processing message_id=%s", message_id)
        raise
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            logging.warning("Graph returned 404 during processing. Skipping message_id=%s", message_id)
            return "skipped"
        logging.exception("HTTP error while processing message_id=%s", message_id)
        raise
    except Exception:
        logging.exception("PROCESSOR FAILED for message_id=%s", message_id)
        raise