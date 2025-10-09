import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
import os
import csv

# Default schema
DEFAULT_SETTINGS = {
    "name": "default-router",
    "method": "GET",
    "url": "https://api.example.com/restapi/v4/orders/",
    "params": {
        "limit": 10,
        "offset": 0,
        "part_number": ""
    },
    "headers": {
        "Content-Type": "application/json",
        "Accept": "application/json"
    },
    "body_mode": "json",
    "body": {},
    "timeout": 10,
    "sort_enabled": False,
    "sort": "desc",
    "status_enabled": False,
    "status": "",
    "rate_limit_enabled": True,
    "requests_per_minute": 60,
    "batch_size": 10,
    "max_retries": 3,
    "allow_redirects": True,
    "verify": True,
    "username": "your-username",
    "password": "your-password"
}

SETTINGS_FILE = "settings.json"

def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                settings = json.load(f)
            for key, value in DEFAULT_SETTINGS.items():
                if key not in settings:
                    settings[key] = value
            return settings
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load settings: {e}")
            return DEFAULT_SETTINGS.copy()
    else:
        return DEFAULT_SETTINGS.copy()

def save_settings():
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(settings, f, indent=4)
        messagebox.showinfo("Success", "Settings saved successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to save settings: {e}")

def create_labeled_entry(parent, label_text, row, default_value=""):
    tk.Label(parent, text=label_text).grid(row=row, column=0, sticky="e")
    entry = tk.Entry(parent, width=40)
    entry.insert(0, default_value)
    entry.grid(row=row, column=1)
    return entry

def create_labeled_spinbox(parent, label_text, row, from_, to, default_value):
    tk.Label(parent, text=label_text).grid(row=row, column=0, sticky="e")
    spinbox = tk.Spinbox(parent, from_=from_, to=to, width=10)
    spinbox.delete(0, "end")
    spinbox.insert(0, default_value)
    spinbox.grid(row=row, column=1)
    return spinbox

def create_labeled_checkbox(parent, label_text, row, var):
    tk.Checkbutton(parent, text=label_text, variable=var).grid(row=row, column=0, columnspan=2, sticky="w")
import requests
import base64
import time
import logging

def export_to_csv(orders):
    if not orders:
        messagebox.showwarning("No Data", "No orders found for the given part number.")
        return
    file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    if not file_path:
        return
    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Order Reference", "Order Date", "Status", "Item Name", "Quantity", "Total",
            "Shipping Full Name", "Address Line 1", "City", "State", "Postal Code", "Country"
        ])
        for order in orders:
            item = order["items"][0] if order["items"] else {}
            shipping = order.get("shipping_address", {})
            writer.writerow([
                order.get("order_reference", ""),
                order.get("order_date", ""),
                order.get("status", ""),
                item.get("name", ""),
                item.get("quantity", ""),
                order.get("total", ""),
                shipping.get("full_name", ""),
                shipping.get("line_1", ""),
                shipping.get("city", ""),
                shipping.get("state", ""),
                shipping.get("postal_code", ""),
                shipping.get("country", "")
            ])
    messagebox.showinfo("Success", f"Orders exported to {file_path}")

def on_fetch_orders():
    part_number = part_entry.get().strip()
    if not part_number:
        messagebox.showerror("Error", "Please enter a part number.")
        return
    fetch_button.config(state="disabled")
    root.update()
    orders = fetch_all_orders(part_number)
    total_orders = len(orders)
    total_value = sum(float(order.get("total", 0)) for order in orders)
    progress_label.config(text=f"Total Orders: {total_orders} | Total Value: ${total_value:.2f}")
    file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    if file_path:
        export_to_csv(orders, file_path)
        messagebox.showinfo("Success", f"Orders exported to {file_path}")
    fetch_button.config(state="normal")
def fetch_all_orders(part_number):
    client = APIClient(settings)
    all_orders = []
    offset = 0
    limit = settings["params"].get("limit", 10)
    batch_size = settings.get("batch_size", 10)
    while True:
        settings["params"]["part_number"] = part_number
        settings["params"]["offset"] = offset
        settings["params"]["limit"] = limit
        orders_batch = client.fetch_paginated_data(lambda msg: progress_label.config(text=msg))
        if not orders_batch:
            break
        all_orders.extend(orders_batch)
        if len(orders_batch) < batch_size:
            break
        offset += batch_size
    return all_orders
class APIClient:
    def __init__(self, settings):
        self.settings = settings
        self.session = requests.Session()
        self._configure_session()

    def _configure_session(self):
        headers = self.settings.get("headers", {})
        auth_string = f"{self.settings['username']}:{self.settings['password']}"
        auth_header = base64.b64encode(auth_string.encode()).decode()
        headers["Authorization"] = f"Basic {auth_header}"
        self.session.headers.update(headers)

    def fetch_paginated_data(self, progress_callback=None):
        offset = int(self.settings["params"].get("offset", 0))
        all_results = []
        total_count = None

        rpm = int(self.settings.get("requests_per_minute", 60))
        delay = 60 / rpm if self.settings.get("rate_limit_enabled", False) else 0
        batch_size = self.settings.get("batch_size", 10)
        max_retries = self.settings.get("max_retries", 3)

        while True:
            self.settings["params"]["offset"] = str(offset)
            for attempt in range(max_retries):
                try:
                    if progress_callback:
                        progress_callback(f"Fetching offset {offset}...")
                    response = self.session.request(
                        method=self.settings.get("method", "GET"),
                        url=self.settings.get("url"),
                        params=self.settings.get("params"),
                        timeout=self.settings.get("timeout", 10),
                        allow_redirects=self.settings.get("allow_redirects", True),
                        verify=self.settings.get("verify", True)
                    )
                    if response.status_code == 200:
                        data = response.json()
                        if total_count is None:
                            total_count = data.get("count", 0)
                        batch = data.get("results", [])
                        all_results.extend(batch)
                        logging.info(f"Fetched {len(batch)} items at offset {offset}")
                        break
                    else:
                        logging.warning(f"Failed with status {response.status_code} at offset {offset}")
                        time.sleep(2 ** attempt)
                except Exception as e:
                    logging.error(f"Exception at offset {offset}: {e}")
                    time.sleep(2 ** attempt)
            else:
                logging.error(f"Max retries exceeded at offset {offset}")
                break

            if len(batch) < batch_size:
                break
            offset += batch_size
            if delay:
                time.sleep(delay)

        return all_results
# GUI setup
root = tk.Tk()
root.title("API Router Settings Editor")
root.geometry("600x800")
notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True)

settings_tab = ttk.Frame(notebook)
notebook.add(settings_tab, text="Settings")

fetch_tab = ttk.Frame(notebook)
notebook.add(fetch_tab, text="Fetch Orders")

settings = load_settings()

# Settings tab entries
name_entry = create_labeled_entry(settings_tab, "Name:", 0, settings["name"])
method_entry = create_labeled_entry(settings_tab, "Method:", 1, settings["method"])
url_entry = create_labeled_entry(settings_tab, "URL:", 2, settings["url"])
limit_spin = create_labeled_spinbox(settings_tab, "Limit:", 3, 1, 1000, settings["params"]["limit"])
offset_spin = create_labeled_spinbox(settings_tab, "Offset:", 4, 0, 1000, settings["params"]["offset"])
part_entry = create_labeled_entry(settings_tab, "Part Number:", 5, settings["params"]["part_number"])
content_type_entry = create_labeled_entry(settings_tab, "Content-Type:", 6, settings["headers"]["Content-Type"])
accept_entry = create_labeled_entry(settings_tab, "Accept:", 7, settings["headers"]["Accept"])
body_mode_entry = create_labeled_entry(settings_tab, "Body Mode:", 8, settings["body_mode"])
body_entry = create_labeled_entry(settings_tab, "Body (JSON):", 9, json.dumps(settings["body"]))
timeout_spin = create_labeled_spinbox(settings_tab, "Timeout:", 10, 1, 60, settings["timeout"])

sort_var = tk.BooleanVar(value=settings["sort_enabled"])
create_labeled_checkbox(settings_tab, "Enable Sorting", 11, sort_var)
sort_entry = create_labeled_entry(settings_tab, "Sort Order:", 12, settings["sort"])

status_var = tk.BooleanVar(value=settings["status_enabled"])
create_labeled_checkbox(settings_tab, "Enable Status Filter", 13, status_var)
status_entry = create_labeled_entry(settings_tab, "Status:", 14, settings["status"])

rate_var = tk.BooleanVar(value=settings["rate_limit_enabled"])
create_labeled_checkbox(settings_tab, "Enable Rate Limiting", 15, rate_var)
rate_spin = create_labeled_spinbox(settings_tab, "Requests/Minute:", 16, 1, 1000, settings["requests_per_minute"])
batch_spin = create_labeled_spinbox(settings_tab, "Batch Size:", 17, 1, 100, settings["batch_size"])
retry_spin = create_labeled_spinbox(settings_tab, "Max Retries:", 18, 1, 10, settings["max_retries"])

allow_var = tk.BooleanVar(value=settings["allow_redirects"])
create_labeled_checkbox(settings_tab, "Allow Redirects", 19, allow_var)
verify_var = tk.BooleanVar(value=settings["verify"])
create_labeled_checkbox(settings_tab, "Verify SSL", 20, verify_var)

user_entry = create_labeled_entry(settings_tab, "Username:", 21, settings["username"])
pass_entry = create_labeled_entry(settings_tab, "Password:", 22, settings["password"])
client = APIClient(settings)
fetch_button = ttk.Button(fetch_tab, text="Fetch Orders", command=on_fetch_orders)
fetch_button.pack(pady=10)


def on_save():
    settings["name"] = name_entry.get()
    settings["method"] = method_entry.get()
    settings["url"] = url_entry.get()
    settings["params"]["limit"] = int(limit_spin.get())
    settings["params"]["offset"] = int(offset_spin.get())
    settings["params"]["part_number"] = part_entry.get()
    settings["headers"]["Content-Type"] = content_type_entry.get()
    settings["headers"]["Accept"] = accept_entry.get()
    settings["body_mode"] = body_mode_entry.get()
    try:
        settings["body"] = json.loads(body_entry.get())
    except json.JSONDecodeError:
        messagebox.showerror("Error", "Body must be valid JSON.")
        return
    settings["timeout"] = int(timeout_spin.get())
    settings["sort_enabled"] = sort_var.get()
    settings["sort"] = sort_entry.get()
    settings["status_enabled"] = status_var.get()
    settings["status"] = status_entry.get()
    settings["rate_limit_enabled"] = rate_var.get()
    settings["requests_per_minute"] = int(rate_spin.get())
    settings["batch_size"] = int(batch_spin.get())
    settings["max_retries"] = int(retry_spin.get())
    settings["allow_redirects"] = allow_var.get()
    settings["verify"] = verify_var.get()
    settings["username"] = user_entry.get()
    settings["password"] = pass_entry.get()
    save_settings()

save_button = ttk.Button(settings_tab, text="Save Settings", command=on_save)
save_button.grid(row=23, column=0, columnspan=2, pady=10)
progress_label = tk.Label(fetch_tab, text="Click to fetch orders using settings.json")
progress_label.pack(pady=10)
root.mainloop()
