import tkinter as tk
from tkinter import messagebox
import login_backend
from db import set_dsn
from main_interface import MainInterface

def do_login():
    username = entry_username.get().strip()
    password = entry_password.get().strip()

    if not username or not password:
        messagebox.showerror("Login Failed", "Enter username and password.")
        return

    if login_backend.check_admin(username, password):
        messagebox.showinfo("Login Successful", f"Welcome, {username} (Admin)!")
        set_dsn("super")   # we changed the global variable so it is now in admin mode
        root.destroy()
        MainInterface("super")
        return

    if login_backend.check_user(username, password):
        messagebox.showinfo("Login Successful", f"Welcome, {username}!")
        set_dsn("normal")  # read-only user connection
        root.destroy()
        MainInterface("normal")
        return

    messagebox.showerror("Login Failed", "Invalid credentials. Please try again.")

def open_register():
    top = tk.Toplevel(root)
    top.title("Register Normal User")
    top.geometry("320x220")

    name_var = tk.StringVar()
    p1_var   = tk.StringVar()
    p2_var   = tk.StringVar()

    tk.Label(top, text="Name").pack(pady=4)
    tk.Entry(top, textvariable=name_var).pack(pady=4)

    tk.Label(top, text="Password").pack(pady=4)
    tk.Entry(top, textvariable=p1_var, show="*").pack(pady=4)

    tk.Label(top, text="Confirm Password").pack(pady=4)
    tk.Entry(top, textvariable=p2_var, show="*").pack(pady=4)

    def do_register():
        name = name_var.get().strip()
        p1   = p1_var.get().strip()
        p2   = p2_var.get().strip()

        if not name or not p1:
            messagebox.showerror("Error", "Name and password are required.")
            return
        if p1 != p2:
            messagebox.showerror("Error", "Passwords do not match.")
            return
        try:
            login_backend.insert_user(name, p1)
            messagebox.showinfo("OK", "Registered! You can log in now.")
            top.destroy()
        except Exception as e:
            # Likely duplicate name (unique constraint) or DB error
            messagebox.showerror("DB error", str(e))

    tk.Button(top, text="Register", command=do_register).pack(pady=10)

# --- login window ---
root = tk.Tk()
root.title("Login")
root.geometry("320x220")
root.attributes("-topmost", True); root.after(150, lambda: root.attributes("-topmost", False))

tk.Label(root, text="Username:").pack(pady=5)
entry_username = tk.Entry(root); entry_username.pack(pady=5)

tk.Label(root, text="Password:").pack(pady=5)
entry_password = tk.Entry(root, show="*"); entry_password.pack(pady=5)

btns = tk.Frame(root); btns.pack(pady=12)
tk.Button(btns, text="Login",    width=10, command=do_login).grid(row=0, column=0, padx=6)
tk.Button(btns, text="Register", width=10, command=open_register).grid(row=0, column=1, padx=6)

root.mainloop()
