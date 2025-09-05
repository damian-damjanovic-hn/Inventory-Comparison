"""
Inventory Reconcile — HNAU vs VS (Minimal UI, Column-Pair Mapping)

- Pick keys (1–2 per side) and map compare pairs via dropdowns
  even if column names differ (e.g., HNAU: FreeStock2 ↔ VS: FreeStock).
- First compare pair is the "quantity" for In/Out stats.
- Save/Load JSON config.
- Efficient on large CSVs: reads only keys + mapped columns.

Deps: tkinter (stdlib), pandas, json (stdlib)
Run:  python inventory_reconcile_gui.py
"""
from __future__ import annotations

import os
import json
import queue
import time
import threading
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Tuple, Dict, List

import pandas as pd
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

def read_csv_smart(path: str, usecols: Optional[List[str]] = None, nrows: Optional[int] = None, dtype="string") -> pd.DataFrame:
    encodings = ["utf-8", "utf-8-sig", "latin-1"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc, usecols=usecols, nrows=nrows, dtype=dtype, on_bad_lines="skip")
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Failed to read CSV '{path}': {last_err}")

def parse_qty_to_int(x: object) -> int:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0
    s = str(x).strip()
    if s == "" or s.upper() in {"NULL", "NAN"}:
        return 0
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    s = s.replace(",", "")
    try:
        d = Decimal(s)
        i = int(d.to_integral_value(rounding=ROUND_HALF_UP))
        return -i if neg else i
    except (InvalidOperation, ValueError):
        try:
            i = int(float(s))
            return -i if neg else i
        except Exception:
            return 0

def to_nullable_int_series(series: pd.Series) -> pd.Series:
    return series.map(parse_qty_to_int).astype("Int64")

# ---------------------------- Heuristics -------------------------------------

PK_HINTS = ("sku", "supplier_sku", "part_number", "product", "item", "barcode", "code", "id")
ACC_HINTS = ("account", "supplier", "vendor", "sap")

def guess_primary_key(cols: List[str]) -> Optional[str]:
    for needle in ("sku", "supplier_sku", "part_number"):
        for c in cols:
            if c.lower() == needle:
                return c
    for c in cols:
        if any(h in c.lower() for h in PK_HINTS):
            return c
    return None

def guess_account_key(cols: List[str]) -> Optional[str]:
    for needle in ("account", "supplier_id", "sap_supplier_id", "vendor_id"):
        for c in cols:
            if c.lower() == needle:
                return c
    for c in cols:
        if any(h in c.lower() for h in ACC_HINTS):
            return c
    return None

# --------------------------- Diff / Compare engine ---------------------------

def compute_diff_pairs(
    df_src: pd.DataFrame,
    df_tgt: pd.DataFrame,
    src_keys: List[str],
    tgt_keys: List[str],
    compare_pairs: List[Tuple[str, str]],  # (src_col, tgt_col)
) -> Tuple[pd.DataFrame, Dict[str, int], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not src_keys or not tgt_keys or len(src_keys) != len(tgt_keys):
        raise ValueError("Key mapping must have the same number of columns on both sides (1 or 2).")
    if not compare_pairs:
        raise ValueError("Add at least one compare column pair.")

    # Keep only required columns
    src_keep = list(dict.fromkeys([*src_keys, *[s for s, _ in compare_pairs]]))
    tgt_keep = list(dict.fromkeys([*tgt_keys, *[t for _, t in compare_pairs]]))
    A = df_src[src_keep].copy()
    B = df_tgt[tgt_keep].copy()

    for s, t in compare_pairs:
        if s in A: A[s] = to_nullable_int_series(A[s])
        if t in B: B[t] = to_nullable_int_series(B[t])

    # Unified join keys
    kcols = [f"k{i}" for i in range(len(src_keys))]
    A = A.rename(columns={src_keys[i]: kcols[i] for i in range(len(src_keys))})
    B = B.rename(columns={tgt_keys[i]: kcols[i] for i in range(len(tgt_keys))})

    # Canonical value names: val{i}_src / val{i}_tgt
    val_src_names, val_tgt_names = {}, {}
    for i, (s, t) in enumerate(compare_pairs):
        val_src_names[s] = f"val{i}_src"
        val_tgt_names[t] = f"val{i}_tgt"
    A = A.rename(columns=val_src_names)
    B = B.rename(columns=val_tgt_names)

    merged = pd.merge(A, B, on=kcols, how="outer", indicator=True)
    merged["key"] = merged[kcols].astype("string").fillna("").agg(" | ".join, axis=1)

    # equal across all pairs
    equal_all = pd.Series(True, index=merged.index)
    for i, _ in enumerate(compare_pairs):
        s = merged.get(f"val{i}_src")
        t = merged.get(f"val{i}_tgt")
        eq = (s.isna() & t.isna()) | (s.fillna(0) == t.fillna(0))
        equal_all &= eq

    only_src_mask = merged["_merge"].eq("left_only")
    only_tgt_mask = merged["_merge"].eq("right_only")
    both_mask     = merged["_merge"].eq("both")
    mismatch_mask = both_mask & (~equal_all)

    # mismatches (human-friendly column labels)
    out_cols = ["key", "change"]
    diff_df = merged[mismatch_mask].copy()
    diff_df.insert(0, "change", "modified")
    for i, (s_name, t_name) in enumerate(compare_pairs):
        diff_df[f"{s_name}_src"] = diff_df.get(f"val{i}_src")
        diff_df[f"{t_name}_tgt"] = diff_df.get(f"val{i}_tgt")
        out_cols.extend([f"{s_name}_src", f"{t_name}_tgt"])
    diff_df = diff_df.reindex(columns=out_cols)

    # only-in sets (show first pair where possible)
    only_src = merged[only_src_mask].copy()
    only_tgt = merged[only_tgt_mask].copy()
    s0, t0 = compare_pairs[0]
    src_cols = ["key"] + (["val0_src"] if "val0_src" in only_src else [])
    tgt_cols = ["key"] + (["val0_tgt"] if "val0_tgt" in only_tgt else [])
    only_src = only_src[src_cols].rename(columns={"val0_src": f"{s0}_src"})
    only_tgt = only_tgt[tgt_cols].rename(columns={"val0_tgt": f"{t0}_tgt"})

    # In/Out sets (first pair as qty)
    q_s = merged.get("val0_src").fillna(0)
    q_t = merged.get("val0_tgt").fillna(0)
    src_in_tgt_out = merged[both_mask & (q_s > 0) & (q_t <= 0)][["key", "val0_src", "val0_tgt"]]\
                     .rename(columns={"val0_src": f"{s0}_src", "val0_tgt": f"{t0}_tgt"})
    tgt_in_src_out = merged[both_mask & (q_t > 0) & (q_s <= 0)][["key", "val0_src", "val0_tgt"]]\
                     .rename(columns={"val0_src": f"{s0}_src", "val0_tgt": f"{t0}_tgt"})

    stats = {
        "added":   int(only_tgt_mask.sum()),
        "removed": int(only_src_mask.sum()),
        "modified": int(mismatch_mask.sum()),
        "same":    int((both_mask & equal_all).sum()),
        "hnau_in_vs_out": int(len(src_in_tgt_out)),
        "vs_in_hnau_out": int(len(tgt_in_src_out)),
        "hnau_rows": int(len(A)),
        "vs_rows": int(len(B)),
    }
    return diff_df, stats, only_src, only_tgt, src_in_tgt_out, tgt_in_src_out

def use_simple_theme(root: tk.Tk):
    style = ttk.Style(root)

    for theme in ("vista", "xpnative", "clam", style.theme_use()):
        try:
            style.theme_use(theme)
            break
        except tk.TclError:
            continue

    style.configure("TButton", padding=6, font=("Segoe UI", 10))
    style.configure("TNotebook", padding=0)
    style.configure("TNotebook.Tab", padding=(10, 6), font=("Segoe UI", 10, "italic"))
    style.map("TNotebook.Tab", background=[("selected", "#e0e0e0")])

    style.configure("Treeview",
                    rowheight=24,
                    font=("Segoe UI", 9),
                    borderwidth=1,
                    relief="flat")
    style.configure("Treeview.Heading",
                    font=("Segoe UI", 10, "italic"),
                    background="#f0f0f0",
                    foreground="#333",
                    padding=6)
    style.map("Treeview", background=[("selected", "#cce5ff")])

class KeysAndPairsDialog(tk.Toplevel):
    """
    Pick:
      - Source keys (1 or 2)
      - Target keys (1 or 2)
      - Compare column pairs (multiple rows of Source ↔ Target)
    First pair drives in/out stats.
    """
    def __init__(self, master, cols_src: List[str], cols_tgt: List[str], state: dict):
        super().__init__(master)
        self.title("Keys & Compare Pairs")
        self.transient(master)
        self.grab_set()
        self.state = state
        self.cols_src = list(cols_src)
        self.cols_tgt = list(cols_tgt)
        self._pair_rows: List[Tuple[ttk.Frame, ttk.Combobox, ttk.Combobox]] = []

        frm = ttk.Frame(self, padding=10)
        frm.grid(row=0, column=0, sticky="nsew")
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)
        for i in range(3):
            frm.grid_columnconfigure(i, weight=1)

        # Defaults
        src_k1_d = self.state.get("src_key1") or guess_primary_key(self.cols_src) or (self.cols_src[0] if self.cols_src else "")
        src_k2_d = self.state.get("src_key2") or guess_account_key(self.cols_src) or ""
        tgt_k1_d = self.state.get("tgt_key1") or guess_primary_key(self.cols_tgt) or (self.cols_tgt[0] if self.cols_tgt else "")
        tgt_k2_d = self.state.get("tgt_key2") or guess_account_key(self.cols_tgt) or ""

        ttk.Label(frm, text="Source Key 1").grid(row=0, column=0, sticky="w")
        self.src_k1 = ttk.Combobox(frm, values=self.cols_src, state="readonly"); self.src_k1.set(src_k1_d)
        self.src_k1.grid(row=1, column=0, sticky="ew", padx=(0,6))

        ttk.Label(frm, text="Source Key 2 (optional)").grid(row=2, column=0, sticky="w")
        self.src_k2 = ttk.Combobox(frm, values=self.cols_src, state="readonly")
        if src_k2_d: self.src_k2.set(src_k2_d)
        self.src_k2.grid(row=3, column=0, sticky="ew", padx=(0,6), pady=(0,8))

        ttk.Label(frm, text="Target Key 1").grid(row=0, column=1, sticky="w")
        self.tgt_k1 = ttk.Combobox(frm, values=self.cols_tgt, state="readonly"); self.tgt_k1.set(tgt_k1_d)
        self.tgt_k1.grid(row=1, column=1, sticky="ew", padx=(6,6))

        ttk.Label(frm, text="Target Key 2 (optional)").grid(row=2, column=1, sticky="w")
        self.tgt_k2 = ttk.Combobox(frm, values=self.cols_tgt, state="readonly")
        if tgt_k2_d: self.tgt_k2.set(tgt_k2_d)
        self.tgt_k2.grid(row=3, column=1, sticky="ew", padx=(6,6), pady=(0,8))

        ttk.Label(frm, text="Compare Pairs (Source ↔ Target). First pair used for In/Out stats.")\
            .grid(row=0, column=2, sticky="w")

        self.pairs_frame = ttk.Frame(frm, padding=(0,2))
        self.pairs_frame.grid(row=1, column=2, rowspan=3, sticky="nsew", padx=(6,0))
        self.pairs_frame.columnconfigure(0, weight=1); self.pairs_frame.columnconfigure(1, weight=1)

        controls = ttk.Frame(frm); controls.grid(row=4, column=2, sticky="e", pady=(6,0))
        ttk.Button(controls, text="Add Pair", command=self._add_pair_row).pack(side="left", padx=(0,6))
        ttk.Button(controls, text="Remove Last", command=self._remove_last_pair).pack(side="left")

        existing = self.state.get("compare_pairs") or []
        if existing:
            for s, t in existing:
                self._add_pair_row(s, t)
        else:
            inter = [c for c in self.cols_src if c in self.cols_tgt]
            if inter:
                self._add_pair_row(inter[0], inter[0])
            else:
                self._add_pair_row(self.cols_src[0] if self.cols_src else "", self.cols_tgt[0] if self.cols_tgt else "")

        bottom = ttk.Frame(frm); bottom.grid(row=5, column=0, columnspan=3, sticky="e", pady=(8,0))
        ttk.Button(bottom, text="Cancel", command=self.destroy).pack(side="right", padx=6)
        ttk.Button(bottom, text="Save", command=self._save).pack(side="right")

    def _add_pair_row(self, src_default: str = "", tgt_default: str = ""):
        r = len(self._pair_rows)
        row = ttk.Frame(self.pairs_frame)
        ttk.Label(row, text=f"Pair {r+1}").pack(side="left", padx=(0,6))
        src_cb = ttk.Combobox(row, values=self.cols_src, state="readonly", width=24)
        tgt_cb = ttk.Combobox(row, values=self.cols_tgt, state="readonly", width=24)
        if src_default: src_cb.set(src_default)
        if tgt_default: tgt_cb.set(tgt_default)
        src_cb.pack(side="left", padx=(0,6))
        ttk.Label(row, text="↔").pack(side="left")
        tgt_cb.pack(side="left", padx=(6,0))
        row.grid(row=r, column=0, columnspan=2, sticky="w", pady=2)
        self._pair_rows.append((row, src_cb, tgt_cb))

    def _remove_last_pair(self):
        if not self._pair_rows:
            return
        row, src_cb, tgt_cb = self._pair_rows.pop()
        row.destroy()

    def _save(self):
        ks_src = [self.src_k1.get().strip()]
        if self.src_k2.get().strip(): ks_src.append(self.src_k2.get().strip())
        ks_tgt = [self.tgt_k1.get().strip()]
        if self.tgt_k2.get().strip(): ks_tgt.append(self.tgt_k2.get().strip())
        if len(ks_src) != len(ks_tgt):
            messagebox.showwarning("Keys", "Source/Target keys must have the same number of columns (1 or 2).")
            return

        pairs: List[Tuple[str, str]] = []
        for row, src_cb, tgt_cb in self._pair_rows:
            s = (src_cb.get() or "").strip()
            t = (tgt_cb.get() or "").strip()
            if s and t:
                pairs.append((s, t))
        if not pairs:
            messagebox.showwarning("Columns", "Add at least one compare column pair.")
            return

        self.state["src_key1"], self.state["src_key2"] = ks_src[0], (ks_src[1] if len(ks_src) == 2 else None)
        self.state["tgt_key1"], self.state["tgt_key2"] = ks_tgt[0], (ks_tgt[1] if len(ks_tgt) == 2 else None)
        self.state["compare_pairs"] = pairs
        self.destroy()

class App(tk.Tk):
    DEFAULT_CFG = "inventory_reconcile_config.json"

    def __init__(self):
        super().__init__()
        self.title("Inventory Reconcile — HNAU vs VS")
        self.geometry("1180x740")
        self.minsize(980, 640)
        use_simple_theme(self)

        self.src_path = tk.StringVar(value="")
        self.tgt_path = tk.StringVar(value="")
        self.export_dir = tk.StringVar(value=os.path.join(os.path.expanduser("~"), "Downloads"))
        self.mapping_state: Dict[str, object] = {
            "src_key1": None, "src_key2": None,
            "tgt_key1": None, "tgt_key2": None,
            "compare_pairs": None,
        }

        self._result_frames: Dict[str, pd.DataFrame] = {}
        self._log_q: "queue.Queue[str]" = queue.Queue()
        self._worker: Optional[threading.Thread] = None

        self._build_ui()
        self.after(120, self._drain_log_queue)

        if os.path.exists(self.DEFAULT_CFG):
            try:
                self._load_config(self.DEFAULT_CFG)
                self._log(f"Loaded config: {self.DEFAULT_CFG}")
            except Exception as e:
                self._log(f"[WARN] Couldn't load default config: {e}")

    def _build_ui(self):
        self.columnconfigure(0, minsize=320)
        self.columnconfigure(1, weight=1)
        self.rowconfigure(0, weight=0)
        self.rowconfigure(1, weight=1)
        self.rowconfigure(2, weight=0)

        self._build_sidebar()
        self._build_statsbar()
        self._build_tabs()
        self._build_log()

    def _build_sidebar(self):
        side = ttk.Frame(self, padding=10)
        side.grid(row=0, column=0, rowspan=3, sticky="nsw")
        side.grid_propagate(False)

        def row(label, var, pick_cmd):
            ttk.Label(side, text=label).pack(anchor="w", pady=(4,0))
            fr = ttk.Frame(side); fr.pack(fill="x", pady=2)
            tk.Entry(fr, textvariable=var).pack(side="left", fill="x", expand=True)
            ttk.Button(fr, text="Browse…", command=pick_cmd).pack(side="left", padx=(6,0))

        row("HNAU (Source) CSV", self.src_path, self._pick_src)
        row("VS (Target) CSV", self.tgt_path, self._pick_tgt)

        ttk.Button(side, text="Select Keys / Compare Pairs", command=self._open_mapping_dialog).pack(fill="x", pady=(8,2))
        fr_cfg = ttk.Frame(side); fr_cfg.pack(fill="x", pady=(2,2))
        ttk.Button(fr_cfg, text="Load Config", command=self._load_config_dialog).pack(side="left", fill="x", expand=True, padx=(0,4))
        ttk.Button(fr_cfg, text="Save Config", command=self._save_config_dialog).pack(side="left", fill="x", expand=True, padx=(4,0))

        ttk.Label(side, text="Export Folder").pack(anchor="w", pady=(10,0))
        fr_exp = ttk.Frame(side); fr_exp.pack(fill="x", pady=2)
        tk.Entry(fr_exp, textvariable=self.export_dir).pack(side="left", fill="x", expand=True)
        ttk.Button(fr_exp, text="Choose…", command=self._pick_export_dir).pack(side="left", padx=(6,0))

        ttk.Separator(side).pack(fill="x", pady=10)
        ttk.Button(side, text="Run Compare", command=self._run_compare).pack(fill="x")
        ttk.Button(side, text="Export All CSVs", command=self._export_all, state="disabled").pack(fill="x", pady=(6,0))
        self.export_btn = side.pack_slaves()[-1]

    def _build_statsbar(self):
        bar = ttk.Frame(self, padding=(10, 8))
        bar.grid(row=0, column=1, sticky="ew")
        for i in range(5): bar.columnconfigure(i, weight=1)

        self._stat_vars = {
            "HNAU rows": tk.StringVar(value="—"),
            "VS rows": tk.StringVar(value="—"),
            "Mismatches": tk.StringVar(value="—"),
            "HNAU ∧ ¬VS": tk.StringVar(value="—"),
            "VS ∧ ¬HNAU": tk.StringVar(value="—"),
        }

        def add(idx, title):
            cell = ttk.Frame(bar); cell.grid(row=0, column=idx, sticky="ew", padx=6)
            ttk.Label(cell, text=title).pack(anchor="w")
            ttk.Label(cell, textvariable=self._stat_vars[title], font=("Segoe UI", 11, "bold")).pack(anchor="w")

        add(0,"HNAU rows"); add(1,"VS rows"); add(2,"Mismatches"); add(3,"HNAU ∧ ¬VS"); add(4,"VS ∧ ¬HNAU")

    def _build_tabs(self):
        wrap = ttk.Frame(self, padding=(12, 12, 12, 12))
        wrap.grid(row=1, column=1, sticky="nsew")
        wrap.rowconfigure(0, weight=1)
        wrap.columnconfigure(0, weight=1)

        self.nb = ttk.Notebook(wrap)
        self.nb.grid(row=0, column=0, sticky="nsew")

        self.tables: Dict[str, ttk.Treeview] = {}

        tab_names = [
            "Mismatches",
            "Only in HNAU",
            "Only in VS",
            "HNAU in & VS out",
            "VS in & HNAU out"
        ]

        for name in tab_names:
            frame = ttk.Frame(self.nb)
            frame.grid_rowconfigure(1, weight=1)
            frame.grid_columnconfigure(0, weight=1)

            toolbar = ttk.Frame(frame, padding=(8, 6))
            toolbar.grid(row=0, column=0, sticky="ew")
            toolbar.columnconfigure(1, weight=1)

            export_btn = ttk.Button(toolbar, text="Export This", command=lambda n=name: self._export_single(n))
            export_btn.grid(row=0, column=0, sticky="w")

            label = ttk.Label(toolbar, text=name, font=("Segoe UI", 10, "bold"))
            label.grid(row=0, column=1, sticky="w", padx=(12, 0))

            cont = ttk.Frame(frame)
            cont.grid(row=1, column=0, sticky="nsew")
            cont.rowconfigure(0, weight=1)
            cont.columnconfigure(0, weight=1)

            tv = ttk.Treeview(cont, show="headings")
            ys = ttk.Scrollbar(cont, orient="vertical", command=tv.yview)
            xs = ttk.Scrollbar(cont, orient="horizontal", command=tv.xview)
            tv.configure(yscrollcommand=ys.set, xscrollcommand=xs.set)

            tv.grid(row=0, column=0, sticky="nsew")
            ys.grid(row=0, column=1, sticky="ns")
            xs.grid(row=1, column=0, sticky="ew")

            tv.tag_configure("oddrow", background="#f7f7f7")

            self.nb.add(frame, text=name)
            self.tables[name] = tv


    def _build_log(self):
        console = ttk.Frame(self, padding=(10, 4, 10, 10))
        console.grid(row=2, column=1, sticky="ew")
        console.columnconfigure(0, weight=1)
        ttk.Label(console, text="Log").grid(row=0, column=0, sticky="w")
        self.log_text = tk.Text(console, height=5)
        self.log_text.grid(row=1, column=0, sticky="ew")

    def _pick_src(self):
        p = filedialog.askopenfilename(title="Select HNAU CSV", filetypes=[("CSV","*.csv"),("All","*.*")])
        if p: self.src_path.set(p)

    def _pick_tgt(self):
        p = filedialog.askopenfilename(title="Select VS CSV", filetypes=[("CSV","*.csv"),("All","*.*")])
        if p: self.tgt_path.set(p)

    def _pick_export_dir(self):
        p = filedialog.askdirectory(title="Select export folder")
        if p: self.export_dir.set(p)

    def _open_mapping_dialog(self):
        try:
            cols_src = self._peek_columns(self.src_path.get())
            cols_tgt = self._peek_columns(self.tgt_path.get())
        except Exception as e:
            messagebox.showwarning("Columns", f"Load both CSVs first.\n\n{e}")
            return
        KeysAndPairsDialog(self, cols_src, cols_tgt, self.mapping_state)

    def _peek_columns(self, path: str) -> List[str]:
        if not path or not os.path.exists(path):
            raise FileNotFoundError("Missing CSV path.")
        df = read_csv_smart(path, nrows=0)
        return list(df.columns)

    def _load_config_dialog(self):
        p = filedialog.askopenfilename(title="Load Config", filetypes=[("JSON","*.json"),("All","*.*")])
        if p:
            try:
                self._load_config(p)
                messagebox.showinfo("Config", f"Loaded: {p}")
                self._log(f"Loaded config: {p}")
            except Exception as e:
                messagebox.showerror("Config", f"Failed to load: {e}")

    def _save_config_dialog(self):
        p = filedialog.asksaveasfilename(title="Save Config", defaultextension=".json", filetypes=[("JSON","*.json")])
        if p:
            try:
                self._save_config(p)
                messagebox.showinfo("Config", f"Saved: {p}")
                self._log(f"Saved config: {p}")
            except Exception as e:
                messagebox.showerror("Config", f"Failed to save: {e}")

    def _load_config(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        ms = cfg.get("mapping_state", {})
        self.mapping_state["src_key1"] = ms.get("src_key1")
        self.mapping_state["src_key2"] = ms.get("src_key2")
        self.mapping_state["tgt_key1"] = ms.get("tgt_key1")
        self.mapping_state["tgt_key2"] = ms.get("tgt_key2")
        self.mapping_state["compare_pairs"] = [tuple(p) for p in ms.get("compare_pairs", []) if isinstance(p, (list, tuple)) and len(p) == 2]
        paths = cfg.get("paths", {})
        self.src_path.set(paths.get("src_path", "") or self.src_path.get())
        self.tgt_path.set(paths.get("tgt_path", "") or self.tgt_path.get())
        self.export_dir.set(paths.get("export_dir", "") or self.export_dir.get())

    def _save_config(self, path: str):
        cfg = {
            "mapping_state": {
                "src_key1": self.mapping_state.get("src_key1"),
                "src_key2": self.mapping_state.get("src_key2"),
                "tgt_key1": self.mapping_state.get("tgt_key1"),
                "tgt_key2": self.mapping_state.get("tgt_key2"),
                "compare_pairs": self.mapping_state.get("compare_pairs"),
            },
            "paths": {
                "src_path": self.src_path.get(),
                "tgt_path": self.tgt_path.get(),
                "export_dir": self.export_dir.get(),
            },
            "saved_at": datetime.now().isoformat(timespec="seconds"),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)

    def _resolve_keys_and_pairs(self) -> Tuple[List[str], List[Tuple[str, str]]]:
        st = self.mapping_state
        ks_src = [k for k in [st.get("src_key1"), st.get("src_key2")] if k]
        ks_tgt = [k for k in [st.get("tgt_key1"), st.get("tgt_key2")] if k]
        pairs = st.get("compare_pairs") or []
        return [*ks_src, *ks_tgt], pairs

    def _run_compare(self):
        if not (self.src_path.get() and os.path.exists(self.src_path.get())):
            messagebox.showwarning("HNAU", "Select a valid HNAU CSV.")
            return
        if not (self.tgt_path.get() and os.path.exists(self.tgt_path.get())):
            messagebox.showwarning("VS", "Select a valid VS CSV.")
            return

        ks_all, pairs = self._resolve_keys_and_pairs()
        src_keys = [k for k in ks_all[:2] if k]
        tgt_keys = [k for k in ks_all[2:] if k]
        if not src_keys or not tgt_keys or len(src_keys) != len(tgt_keys):
            messagebox.showwarning("Keys", "Select matching key counts (1 or 2) via 'Select Keys / Compare Pairs'.")
            return
        if not pairs:
            messagebox.showwarning("Columns", "Add at least one compare column pair.")
            return

        if self._worker and self._worker.is_alive():
            messagebox.showinfo("Busy", "A compare is already running.")
            return

        for b in self.children.values():
            pass
        self._result_frames.clear()
        self._log("Starting compare…")
        self._worker = threading.Thread(target=self._do_compare, args=(src_keys, tgt_keys, pairs), daemon=True)
        self._worker.start()

    def _do_compare(self, src_keys: List[str], tgt_keys: List[str], pairs: List[Tuple[str, str]]):
        t0 = time.time()
        try:
            src_need = list(dict.fromkeys([*src_keys, *[s for s,_ in pairs]]))
            tgt_need = list(dict.fromkeys([*tgt_keys, *[t for _,t in pairs]]))
            src_df = read_csv_smart(self.src_path.get(), usecols=src_need)
            tgt_df = read_csv_smart(self.tgt_path.get(), usecols=tgt_need)

            miss_src = [c for c in src_need if c not in src_df.columns]
            miss_tgt = [c for c in tgt_need if c not in tgt_df.columns]
            if miss_src or miss_tgt:
                raise KeyError(f"Missing columns.\nSource missing: {miss_src}\nTarget missing: {miss_tgt}")

            self._log("Computing differences…")
            diff_df, stats, only_src, only_tgt, src_in_tgt_out, tgt_in_src_out = compute_diff_pairs(
                src_df, tgt_df, src_keys, tgt_keys, pairs
            )

            self._result_frames = {
                "Mismatches": diff_df,
                "Only in HNAU": only_src,
                "Only in VS": only_tgt,
                "HNAU in & VS out": src_in_tgt_out,
                "VS in & HNAU out": tgt_in_src_out,
            }

            self.after(0, lambda: self._apply_stats_and_tables(stats))
            self._log(f"Done in {time.time()-t0:.2f}s")
        except Exception as e:
            self._log(f"[ERROR] {e}")
            msg = f"{type(e).__name__}: {e}"      # capture now (tk lambda-safe)
            self.after(0, lambda m=msg: messagebox.showerror("Compare failed", m))
        finally:
            self.after(0, self._enable_export_if_ready)

    def _enable_export_if_ready(self):
        if self._result_frames:
            for w in self.children.values():
                pass
            self.export_btn.config(state="normal")

    def _apply_stats_and_tables(self, stats: Dict[str,int]):
        def set_stat(k, v): self._stat_vars[k].set(str(v))
        set_stat("HNAU rows", stats.get("hnau_rows", 0))
        set_stat("VS rows", stats.get("vs_rows", 0))
        set_stat("Mismatches", stats.get("modified", 0))
        set_stat("HNAU ∧ ¬VS", stats.get("hnau_in_vs_out", 0))
        set_stat("VS ∧ ¬HNAU", stats.get("vs_in_hnau_out", 0))

        for name, df in self._result_frames.items():
            self._populate_table(self.tables[name], df)

    def _populate_table(self, tv: ttk.Treeview, df: pd.DataFrame):
        for iid in tv.get_children(): tv.delete(iid)
        cols = list(df.columns)
        tv["columns"] = cols
        for c in cols:
            tv.heading(c, text=c)
            tv.column(c, width=120, stretch=True)

        lim = min(25000, len(df))
        for i, (_, row) in enumerate(df.iloc[:lim].iterrows()):
            def fmt(v):
                try:
                    if pd.isna(v): return ""
                except Exception:
                    pass
                return str(v)
            vals = [fmt(v) for v in row.values]
            tag = "oddrow" if (i % 2) else ""
            tv.insert("", "end", values=vals, tags=(tag,))

        self._autosize_columns(tv, df.head(200))

    def _autosize_columns(self, tv: ttk.Treeview, sample: pd.DataFrame):
        for i, col in enumerate(sample.columns):
            w = max(len(str(col)), *(len(str(v)) for v in sample[col].astype("string").fillna("")))
            tv.column(col, width=max(80, min(300, (w + 2) * 7)))

    def _export_single(self, tab_name: str):
        if tab_name not in self._result_frames:
            messagebox.showinfo("Export", "Run a compare first.")
            return
        df = self._result_frames[tab_name]
        tag = datetime.today().strftime('%d_%m_%Y')
        base = self.export_dir.get() or os.path.join(os.path.expanduser("~"), "Downloads")
        os.makedirs(base, exist_ok=True)
        safe = tab_name.lower().replace(" ", "_").replace("&", "and").replace("∧","and").replace("/","_")
        path = os.path.join(base, f"{safe}_{tag}.csv")
        try:
            df.to_csv(path, index=False)
            self._log(f"Exported {tab_name}: {path}")
            messagebox.showinfo("Export complete", f"Saved: {path}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    def _export_all(self):
        if not self._result_frames:
            messagebox.showinfo("Export", "Run a compare first.")
            return
        tag = datetime.today().strftime('%d_%m_%Y')
        base = self.export_dir.get() or os.path.join(os.path.expanduser("~"), "Downloads")
        os.makedirs(base, exist_ok=True)
        for name, df in self._result_frames.items():
            safe = name.lower().replace(" ", "_").replace("&", "and").replace("∧","and").replace("/","_")
            path = os.path.join(base, f"{safe}_{tag}.csv")
            try:
                df.to_csv(path, index=False)
                self._log(f"Exported: {path}")
            except Exception as e:
                self._log(f"[ERROR] Failed to export {name}: {e}")
        messagebox.showinfo("Export complete", f"CSV files saved to:\n{base}")

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self._log_q.put(f"[{ts}] {msg}\n")

    def _drain_log_queue(self):
        try:
            while True:
                line = self._log_q.get_nowait()
                self.log_text.insert("end", line)
                self.log_text.see("end")
        except queue.Empty:
            pass
        finally:
            self.after(150, self._drain_log_queue)

def main():
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()
