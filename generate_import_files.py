import ttkbootstrap as tb
from ttkbootstrap.constants import *
import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog
import pandas as pd
import os

# Default source codes
DEFAULT_SOURCE_CODES = ['pos_337', 'src_virtualstock']

class M2StockApp(tb.Window):
    def __init__(self):
        super().__init__(themename="darkly")
        self.title("M2 Stock Import CSV Generator")
        self.geometry("1200x700")
        self.m2_df = None
        self.original_file_path = None
        self.source_codes = DEFAULT_SOURCE_CODES.copy()
        self.output_folder = os.path.expanduser("~/Desktop")
        self.chunk_size = 1000
        self.use_raw_sku = tb.BooleanVar(value=False)
        self.available_columns = []
        self.sku_column = tb.StringVar()
        self.qty_column = tb.StringVar()

        self.build_ui()

    # def build_ui(self):
    #     # Sidebar for stats
    #     sidebar = tb.Frame(self, width=200, padding=10)
    #     sidebar.pack(side=LEFT, fill=Y)
    #     self.stats_label = tb.Label(sidebar, text="Stats:\n\nNo file loaded", justify=LEFT)
    #     self.stats_label.pack(anchor=NW)

    #     # Main area with tabs
    #     main_area = tb.Frame(self)
    #     main_area.pack(side=RIGHT, fill=BOTH, expand=True)

    #     notebook = tb.Notebook(main_area)
    #     notebook.pack(fill=BOTH, expand=True)

    #     # Configuration tab
    #     tab_config = tb.Frame(notebook, padding=10)
    #     notebook.add(tab_config, text="Configuration")

    #     # File selection
    #     tb.Label(tab_config, text="Select CSV File:").grid(row=0, column=0, sticky=W)
    #     self.entry_file_path = tb.Entry(tab_config, width=60)
    #     self.entry_file_path.grid(row=0, column=1, sticky=W)
    #     tb.Button(tab_config, text="Browse", command=self.select_file).grid(row=0, column=2, padx=5)

    #     self.load_button = tb.Button(self.tab_config, text="Load", command=self.load_data)
    #     self.load_button.config(state='disabled')
    #     self.load_button.pack(pady=10)

    #     # Column mapping
    #     tb.Label(tab_config, text="SKU Column:").grid(row=1, column=0, sticky=W, pady=(10, 0))
    #     self.dropdown_sku = tb.Combobox(tab_config, textvariable=self.sku_column, state="readonly", width=30)
    #     self.dropdown_sku.grid(row=1, column=1, sticky=W)
    #     self.dropdown_sku.bind("<<ComboboxSelected>>", self.check_column_selection)
    #     self.dropdown_qty.bind("<<ComboboxSelected>>", self.check_column_selection)

    #     tb.Label(tab_config, text="Qty Column:").grid(row=2, column=0, sticky=W)
    #     self.dropdown_qty = tb.Combobox(tab_config, textvariable=self.qty_column, state="readonly", width=30)
    #     self.dropdown_qty.grid(row=2, column=1, sticky=W)

    #     tb.Checkbutton(tab_config, text="Use 'key' as SKU (no split)", variable=self.use_raw_sku).grid(row=3, column=1, sticky=W, pady=5)

    #     # Source code manager
    #     source_frame = tb.LabelFrame(tab_config, text="Source Codes", padding=10)
    #     source_frame.grid(row=1, column=2, rowspan=3, padx=10, sticky=N)

    #     self.listbox_sources = tk.Listbox(source_frame, height=5, bg="#2b2b2b", fg="white", selectbackground="#444")
    #     self.listbox_sources.pack(fill=X)
    #     self.refresh_source_list()

    #     btn_frame = tb.Frame(source_frame)
    #     btn_frame.pack(pady=5)
    #     tb.Button(btn_frame, text="Add", command=self.add_source_code).grid(row=0, column=0)
    #     tb.Button(btn_frame, text="Remove", command=self.remove_source_code).grid(row=0, column=1)
    #     tb.Button(btn_frame, text="Reset", command=self.reset_source_codes).grid(row=0, column=2)

    #     # Export settings
    #     tb.Label(tab_config, text="Output Folder:").grid(row=4, column=0, sticky=W, pady=(20, 0))
    #     self.entry_output_folder = tb.Entry(tab_config, width=60)
    #     self.entry_output_folder.insert(0, self.output_folder)
    #     self.entry_output_folder.grid(row=4, column=1, sticky=W)
    #     tb.Button(tab_config, text="Choose", command=self.choose_output_folder).grid(row=4, column=2, padx=5)

    #     tb.Label(tab_config, text="Chunk Size:").grid(row=5, column=0, sticky=W)
    #     self.entry_chunk_size = tb.Entry(tab_config, width=10)
    #     self.entry_chunk_size.insert(0, str(self.chunk_size))
    #     self.entry_chunk_size.grid(row=5, column=1, sticky=W)

    #     tb.Button(tab_config, text="Export M2 CSV", bootstyle=SUCCESS, command=self.export_csv).grid(row=6, column=1, sticky=W, pady=20)

    #     # Preview tab
    #     tab_preview = tb.Frame(notebook, padding=10)
    #     notebook.add(tab_preview, text="Preview")

    #     self.tree = tb.Treeview(tab_preview)
    #     self.tree.pack(fill=BOTH, expand=True)


    def build_ui(self):
        # Sidebar for stats
        sidebar = tb.Frame(self, width=200, padding=10)
        sidebar.pack(side=LEFT, fill=Y)
        self.stats_label = tb.Label(sidebar, text="Stats:\n\nNo file loaded", justify=LEFT)
        self.stats_label.pack(anchor=NW)

        # Main area with tabs
        main_area = tb.Frame(self)
        main_area.pack(side=RIGHT, fill=BOTH, expand=True)

        notebook = tb.Notebook(main_area)
        notebook.pack(fill=BOTH, expand=True)

        # Configuration tab
        tab_config = tb.Frame(notebook, padding=10)
        notebook.add(tab_config, text="Configuration")

        # File selection
        tb.Label(tab_config, text="Select CSV File:").grid(row=0, column=0, sticky=W)
        self.entry_file_path = tb.Entry(tab_config, width=60)
        self.entry_file_path.grid(row=0, column=1, sticky=W)
        tb.Button(tab_config, text="Browse", command=self.select_file).grid(row=0, column=2, padx=5)

        # Load button
        self.load_button = tb.Button(tab_config, text="Load", command=self.load_data)
        self.load_button.config(state='disabled')
        self.load_button.grid(row=0, column=3, padx=5)

        # Column mapping
        tb.Label(tab_config, text="SKU Column:").grid(row=1, column=0, sticky=W, pady=(10, 0))
        self.dropdown_sku = tb.Combobox(tab_config, textvariable=self.sku_column, state="readonly", width=30)
        self.dropdown_sku.grid(row=1, column=1, sticky=W)
        self.dropdown_sku.bind("<<ComboboxSelected>>", self.check_column_selection)

        tb.Label(tab_config, text="Qty Column:").grid(row=2, column=0, sticky=W)
        self.dropdown_qty = tb.Combobox(tab_config, textvariable=self.qty_column, state="readonly", width=30)
        self.dropdown_qty.grid(row=2, column=1, sticky=W)
        self.dropdown_qty.bind("<<ComboboxSelected>>", self.check_column_selection)

        tb.Checkbutton(tab_config, text="Use 'key' as SKU (no split)", variable=self.use_raw_sku).grid(row=3, column=1, sticky=W, pady=5)

        # Source code manager
        source_frame = tb.LabelFrame(tab_config, text="Source Codes", padding=10)
        source_frame.grid(row=1, column=2, rowspan=3, padx=10, sticky=N)

        self.listbox_sources = tk.Listbox(source_frame, height=5, bg="#2b2b2b", fg="white", selectbackground="#444")
        self.listbox_sources.pack(fill=X)
        self.refresh_source_list()

        btn_frame = tb.Frame(source_frame)
        btn_frame.pack(pady=5)
        tb.Button(btn_frame, text="Add", command=self.add_source_code).grid(row=0, column=0)
        tb.Button(btn_frame, text="Remove", command=self.remove_source_code).grid(row=0, column=1)
        tb.Button(btn_frame, text="Reset", command=self.reset_source_codes).grid(row=0, column=2)

        # Export settings
        tb.Label(tab_config, text="Output Folder:").grid(row=4, column=0, sticky=W, pady=(20, 0))
        self.entry_output_folder = tb.Entry(tab_config, width=60)
        self.entry_output_folder.insert(0, self.output_folder)
        self.entry_output_folder.grid(row=4, column=1, sticky=W)
        tb.Button(tab_config, text="Choose", command=self.choose_output_folder).grid(row=4, column=2, padx=5)

        tb.Label(tab_config, text="Chunk Size:").grid(row=5, column=0, sticky=W)
        self.entry_chunk_size = tb.Entry(tab_config, width=10)
        self.entry_chunk_size.insert(0, str(self.chunk_size))
        self.entry_chunk_size.grid(row=5, column=1, sticky=W)

        tb.Button(tab_config, text="Export M2 CSV", bootstyle=SUCCESS, command=self.export_csv).grid(row=6, column=1, sticky=W, pady=20)

        # Preview tab
        tab_preview = tb.Frame(notebook, padding=10)
        notebook.add(tab_preview, text="Preview")

        self.tree = tb.Treeview(tab_preview)
        self.tree.pack(fill=BOTH, expand=True)


    def refresh_source_list(self):
        self.listbox_sources.delete(0, 'end')
        for code in self.source_codes:
            self.listbox_sources.insert('end', code)

    def add_source_code(self):
        new_code = simpledialog.askstring("Add Source Code", "Enter new source_code:")
        if new_code:
            self.source_codes.append(new_code.strip())
            self.refresh_source_list()

    def remove_source_code(self):
        selected = self.listbox_sources.curselection()
        if selected:
            del self.source_codes[selected[0]]
            self.refresh_source_list()

    def reset_source_codes(self):
        self.source_codes = DEFAULT_SOURCE_CODES.copy()
        self.refresh_source_list()

    def choose_output_folder(self):
        folder = filedialog.askdirectory()
        if folder:
            self.output_folder = folder
            self.entry_output_folder.delete(0, 'end')
            self.entry_output_folder.insert(0, folder)

    def select_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.entry_file_path.delete(0, 'end')
            self.entry_file_path.insert(0, file_path)
            self.original_file_path = file_path
            self.dropdown_sku.bind("<<ComboboxSelected>>", self.check_column_selection)
            self.dropdown_qty.bind("<<ComboboxSelected>>", self.check_column_selection)
            self.load_columns(file_path)
            self.after(100, lambda: self.process_csv(file_path))

    # def load_columns(self, file_path):
    #     try:
    #         df = pd.read_csv(file_path, nrows=1)
    #         self.available_columns = list(df.columns)
    #         self.dropdown_sku['values'] = self.available_columns
    #         self.dropdown_qty['values'] = self.available_columns
    #         if 'key' in self.available_columns:
    #             self.sku_column.set('key')
    #         if 'free_stock_tgt' in self.available_columns:
    #             self.qty_column.set('free_stock_tgt')
    #     except Exception as e:
    #         messagebox.showerror("Error", f"Failed to load columns: {e}")

    def load_data(self):
        # Example placeholder logic
        file_path = self.entry_file_path.get()
        sku_col = self.sku_column.get()
        qty_col = self.qty_column.get()

        if not file_path or not sku_col or not qty_col:
            messagebox.showwarning("Missing Info", "Please select file and both columns.")
            return

        try:
            df = pd.read_csv(file_path)
            # Do something with df, sku_col, qty_col
            self.stats_label.config(text=f"Loaded {len(df)} rows.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data: {e}")

    def load_columns(self, file_path):
        try:
            df = pd.read_csv(file_path, nrows=1)
            self.available_columns = list(df.columns)

            # Populate dropdowns with available columns
            self.dropdown_sku['values'] = self.available_columns
            self.dropdown_qty['values'] = self.available_columns

            # Clear any pre-selected values
            self.sku_column.set('')
            self.qty_column.set('')
            
            messagebox.showinfo("Columns Loaded", "Columns loaded successfully. Please select the desired columns.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load columns: {e}")    


    def check_column_selection(self, *_):
        sku_selected = self.sku_column.get().strip()
        qty_selected = self.qty_column.get().strip()

        if sku_selected and qty_selected:
            self.load_button.config(state='normal')  # Enable the button
        else:
            self.load_button.config(state='disabled')  # Disable the button



    def process_csv(self, file_path):
        try:
            df = pd.read_csv(file_path)
            sku_col = self.sku_column.get()
            qty_col = self.qty_column.get()

            if not sku_col or not qty_col:
                # messagebox.showwarning("Warning", "Please select both SKU and Qty columns.")
                return

            if self.use_raw_sku.get():
                df['sku'] = df[sku_col]
            else:
                df['sku'] = df[sku_col].apply(lambda x: x.split('|')[0].strip() if isinstance(x, str) else x)

            m2_rows = []
            for _, row in df.iterrows():
                sku = row['sku']
                qty = row[qty_col]
                try:
                    qty_val = float(qty)
                    stock_status = 1 if qty_val > 0 else 0
                except:
                    qty_val = 0
                    stock_status = 0

                for source in self.source_codes:
                    m2_rows.append({
                        'sku': sku,
                        'stock_status': stock_status,
                        'source_code': source,
                        'qty': qty_val
                    })

            self.m2_df = pd.DataFrame(m2_rows)
            self.preview_data(self.m2_df.head(50))
            self.update_stats()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to process CSV: {e}")

    def preview_data(self, df):
        for col in self.tree.get_children():
            self.tree.delete(col)
        self.tree["columns"] = list(df.columns)
        self.tree["show"] = "headings"
        for col in df.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100)
        for _, row in df.iterrows():
            self.tree.insert("", "end", values=list(row))

    def update_stats(self):
        if self.m2_df is not None:
            total = len(self.m2_df['sku'].unique())
            in_stock = self.m2_df[self.m2_df['stock_status'] == 1]['sku'].nunique()
            out_stock = self.m2_df[self.m2_df['stock_status'] == 0]['sku'].nunique()
            sources = len(self.source_codes)
            self.stats_label.config(text=f"Stats:\n\nTotal SKUs: {total}\nIn Stock: {in_stock}\nOut of Stock: {out_stock}\nSource Codes: {sources}")

    def export_csv(self):
        if self.m2_df is not None and self.original_file_path:
            try:
                chunk_size = int(self.entry_chunk_size.get())
                base_name = os.path.splitext(os.path.basename(self.original_file_path))[0]
                for i in range(0, len(self.m2_df), chunk_size):
                    chunk = self.m2_df.iloc[i:i+chunk_size]
                    output_name = f"{base_name}_m2_import_part{i//chunk_size + 1}.csv"
                    output_path = os.path.join(self.output_folder, output_name)
                    chunk.to_csv(output_path, index=False)
                messagebox.showinfo("Success", f"Exported to {self.output_folder}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to export CSV: {e}")
        else:
            messagebox.showwarning("Warning", "No data to export")

if __name__ == "__main__":
    app = M2StockApp()
    app.mainloop()
