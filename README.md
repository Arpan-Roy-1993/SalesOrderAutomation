# 📦 Sales Order Automation Pipeline

An end-to-end **Azure-based data pipeline** that ingests email attachments, extracts structured data from PDFs, and stores results in Microsoft Fabric Lakehouse for analytics.

---

## 🚀 Overview

This project automates the processing of sales order documents received via email:

1. **Microsoft Graph Webhook** listens for new emails
2. **Azure Function App** processes incoming notifications
3. **PDF attachments** are extracted and parsed
4. **Azure Document Intelligence** structures the data
5. **Processed data** is written to **Microsoft Fabric Lakehouse**

---

## 🏗️ Architecture

```
Microsoft Graph → Azure Function → PDF Extraction → Document Intelligence → Fabric Lakehouse
```

---

## 🧰 Tech Stack

* **Azure Functions** (Python)
* **Microsoft Graph API**
* **Azure Document Intelligence**
* **Microsoft Fabric Lakehouse**
* **Azure Storage / Table Storage**
* **Python (requests, json, etc.)**

---

## 📂 Project Structure

```
sales-order-webhook/
│
├── function_app.py          # Azure Function entry point (webhook handler)
├── processor.py             # Core business logic (email + PDF parsing)
├── requirements.txt         # Python dependencies
├── subscription-body.json   # (excluded) Graph subscription payload
├── .gitignore               # Prevents secrets from being committed
└── README.md
```

---

## ⚙️ How It Works

### 1. Graph Webhook Subscription

* Subscribes to Outlook inbox events
* Triggers Azure Function when a new email arrives

### 2. Azure Function

* Validates Graph webhook requests
* Extracts message ID
* Calls processing logic

### 3. Processor Logic

* Fetches email + attachments via Graph API
* Sends PDF to Document Intelligence
* Parses structured rows (products, pricing, etc.)

### 4. Data Storage

* Writes output to Fabric Lakehouse:

  * Raw JSON
  * Parsed structured rows
  * Metadata

---

## 📊 Output Files (Lakehouse)

* `product_catalog_raw_results.json` → raw AI output
* `product_catalog_parsed_rows.json` → structured data ✅
* `product_catalog_document_rows.csv` → tabular extraction
* `product_catalog_message_metadata.json` → email metadata

---

## 🔐 Security Best Practices

* Secrets stored in:

  * Azure App Settings
  * `local.settings.json` (not committed)
* `.gitignore` excludes:

  * API keys
  * function keys
  * virtual environments

---

## 🧪 Local Development

```bash
# create virtual environment
python -m venv .venv

# activate
.venv\Scripts\activate

# install dependencies
pip install -r requirements.txt

# run function locally
func start
```

---

## ☁️ Deployment

```bash
# deploy to Azure
func azure functionapp publish <your-function-app-name>
```

---

## 💡 Key Learnings

* Event-driven architecture using **Graph webhooks**
* Handling **duplicate event processing**
* Parsing semi-structured PDFs with AI
* Writing scalable pipelines into **Fabric Lakehouse**
* Managing secrets and secure deployments

---

## 🔮 Future Improvements

* Convert parsed data into **Delta Tables**
* Add **deduplication via message ID tracking**
* Implement **retry + error handling**
* Add **dashboarding (Power BI / Fabric)**

---

## 👤 Author

**Arpan Roy**
AI / Data / Cloud Solutions Engineer

---

## ⭐ If you found this useful

Give the repo a star and feel free to fork!
