import pandas as pd
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime
import threading

def normalize_to_snake_case(header):
    temp = header.lower()
    temp = temp.replace("[", "_").replace("]", "")
    temp = temp.replace(" ", "_").replace("#", "number")
    temp = temp.replace("-", "_").replace("/", "_").replace(".", "_")
    while "__" in temp:
        temp = temp.replace("__", "_")
    return temp

class DataPrepperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Prepper")
        self.file_path = ""
        self.sheet_names = []
        self.df = None

        tk.Button(root, text="Select Excel or CSV File", command=self.select_file).pack(pady=10)

        self.sheet_var = tk.StringVar()
        self.sheet_dropdown = ttk.Combobox(root, textvariable=self.sheet_var, state="readonly")
        self.sheet_dropdown.pack(pady=5)
        self.sheet_dropdown.bind("<<ComboboxSelected>>", self.load_sheet_threaded)

        tk.Label(root, text="Enter header row number (e.g., 3):").pack()
        self.header_entry = tk.Entry(root)
        self.header_entry.pack(pady=5)

        tk.Button(root, text="Preview and Select Columns", command=self.preview_data_threaded).pack(pady=10)

        self.listbox = tk.Listbox(root, selectmode=tk.MULTIPLE, width=50)
        self.listbox.pack(pady=5)

        tk.Button(root, text="Export to CSV", command=self.export_csv_threaded).pack(pady=10)

        self.status_label = tk.Label(root, text="", fg="blue")
        self.status_label.pack(pady=5)

    def set_status(self, message):
        self.status_label.config(text=message)
        self.root.update_idletasks()

    def select_file(self):
        self.file_path = filedialog.askopenfilename(filetypes=[
            ("Excel files", "*.xls;*.xlsx"),
            ("CSV files", "*.csv")
        ])
        if self.file_path:
            self.set_status("Loading file...")
            if self.file_path.endswith(".csv"):
                self.sheet_names = ["CSV"]
            else:
                xls = pd.ExcelFile(self.file_path, engine="openpyxl")
                self.sheet_names = xls.sheet_names
            self.sheet_dropdown["values"] = self.sheet_names
            self.sheet_var.set(self.sheet_names[0])
            self.set_status("File loaded.")

    def load_sheet_threaded(self, event=None):
        threading.Thread(target=self.load_sheet).start()

    def load_sheet(self):
        try:
            self.set_status("Loading sheet...")
            sheet = self.sheet_var.get()
            if sheet == "CSV":
                self.df = pd.read_csv(self.file_path, header=None)
            else:
                self.df = pd.read_excel(self.file_path, sheet_name=sheet, header=None, engine="openpyxl")
            self.set_status("Sheet loaded.")
        except Exception as e:
            self.set_status("")
            messagebox.showerror("Error", f"Failed to load sheet: {str(e)}")

    def preview_data_threaded(self):
        threading.Thread(target=self.preview_data).start()

    def preview_data(self):
        try:
            self.set_status("Processing preview...")
            header_row = int(self.header_entry.get()) - 1
            self.df.columns = self.df.iloc[header_row]
            self.df = self.df.drop(index=list(range(header_row + 1)))
            self.df.columns = [normalize_to_snake_case(str(col)) for col in self.df.columns]
            self.listbox.delete(0, tk.END)
            for col in self.df.columns:
                self.listbox.insert(tk.END, col)
            self.set_status("Preview ready.")
        except Exception as e:
            self.set_status("")
            messagebox.showerror("Error", f"Failed to preview data: {str(e)}")

    def export_csv_threaded(self):
        threading.Thread(target=self.export_csv).start()

    def export_csv(self):
        try:
            self.set_status("Exporting CSV...")
            selected_indices = self.listbox.curselection()
            selected_columns = [self.listbox.get(i) for i in selected_indices]
            df_selected = self.df[selected_columns]
            df_selected = df_selected.drop_duplicates()
            base_name = os.path.splitext(os.path.basename(self.file_path))[0]
            today = datetime.today().strftime("%d_%m_%Y")
            output_name = f"{base_name}_{today}_cleaned.csv"
            output_path = os.path.join(os.path.dirname(self.file_path), output_name)
            df_selected.to_csv(output_path, index=False)
            self.set_status("")
            messagebox.showinfo("Success", f"CSV exported successfully to:\n{output_path}")
        except Exception as e:
            self.set_status("")
            messagebox.showerror("Error", f"Failed to export CSV: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = DataPrepperApp(root)
    root.mainloop()
