import tkinter as tk
from tkinter import messagebox
import re
import json
from datetime import datetime

LIGHT_THEME = {
    "bg": "#f0f0f0",
    "fg": "#000000",
    "entry_bg": "#ffffff",
    "button_bg": "#e0e0e0"
}

DARK_THEME = {
    "bg": "#2e2e2e",
    "fg": "#ffffff",
    "entry_bg": "#3e3e3e",
    "button_bg": "#5e5e5e"
}

current_theme = LIGHT_THEME

def apply_theme():
    root.configure(bg=current_theme["bg"])
    for widget in root.winfo_children():
        apply_widget_theme(widget)

def apply_widget_theme(widget):
    if isinstance(widget, tk.Frame):
        widget.configure(bg=current_theme["bg"])
        for child in widget.winfo_children():
            apply_widget_theme(child)
    elif isinstance(widget, tk.Label):
        widget.configure(bg=current_theme["bg"], fg=current_theme["fg"])
    elif isinstance(widget, tk.Button):
        widget.configure(bg=current_theme["button_bg"], fg=current_theme["fg"])
    elif isinstance(widget, tk.Text):
        widget.configure(bg=current_theme["entry_bg"], fg=current_theme["fg"])

def toggle_theme():
    global current_theme
    current_theme = DARK_THEME if current_theme == LIGHT_THEME else LIGHT_THEME
    apply_theme()

# def process_input():
#     input_text = text_input.get("1.0", tk.END)
#     pattern = r's:\d+:"(.*?)";s:\d+:"(.*?)";'
#     matches = re.findall(pattern, input_text)
#     data_dict = {key: value for key, value in matches if key and value}
#     json_output = json.dumps(data_dict, indent=2)
#     text_output.delete("1.0", tk.END)
#     text_output.insert(tk.END, json_output)

def process_input():
    input_text = text_input.get("1.0", tk.END)

    pattern = r's:\d+:"(.*?)";s:\d+:"(.*?)";'

    matches = re.findall(pattern, input_text)

    data_dict = {}
    for key, value in matches:
        if not any(x in key for x in ['\";', 'a:', 'i:', 'b:', 'N']) and not any(x in value for x in ['a:', 'i:', 'b:', 'N']):
            data_dict[key] = value

    json_output = json.dumps(data_dict, indent=2)

    text_output.delete("1.0", tk.END)
    text_output.insert(tk.END, json_output)

def copy_to_clipboard():
    output_text = text_output.get("1.0", tk.END)
    root.clipboard_clear()
    root.clipboard_append(output_text)
    messagebox.showinfo("Copied", "Output copied to clipboard.")

def save_to_file():
    output_text = text_output.get("1.0", tk.END)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"cleaned_json_{timestamp}.json"
    with open(filename, "w") as f:
        f.write(output_text)
    messagebox.showinfo("Saved", f"Output saved to {filename}")

root = tk.Tk()
root.title("PHP Serialized to JSON Converter")
root.geometry("900x600")

control_frame = tk.Frame(root)
control_frame.pack(side=tk.LEFT, fill=tk.Y, padx=10, pady=10)

tk.Button(control_frame, text="Convert to JSON", command=process_input).pack(fill=tk.X, pady=5)
tk.Button(control_frame, text="Copy to Clipboard", command=copy_to_clipboard).pack(fill=tk.X, pady=5)
tk.Button(control_frame, text="Save to File", command=save_to_file).pack(fill=tk.X, pady=5)
tk.Button(control_frame, text="Toggle Theme", command=toggle_theme).pack(fill=tk.X, pady=5)

main_frame = tk.Frame(root)
main_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=10, pady=10)

tk.Label(main_frame, text="Paste PHP Serialized Data:").pack(anchor="w")
text_input = tk.Text(main_frame, height=10, width=100)
text_input.pack(pady=5, fill=tk.X)

tk.Label(main_frame, text="Cleaned JSON Output:").pack(anchor="w")
text_output = tk.Text(main_frame, height=20, width=100)
text_output.pack(pady=5, fill=tk.BOTH, expand=True)

apply_theme()
root.mainloop()
