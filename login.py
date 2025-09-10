import tkinter as tk
from tkinter import messagebox
import login_backend
from db import set_dsn
from main_interface import MainInterface

import sv_ttk

def do_login():
    """Handle login button click"""
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
    """Open register window"""
    top = tk.Toplevel(root)
    top.title("Register Normal User")
    top.geometry("320x250")

    # Create variables
    name_var = tk.StringVar()
    p1_var   = tk.StringVar()
    p2_var   = tk.StringVar()

    # Create register form
    tk.Label(top, text="Name", font=("Arial", 12)).grid(row=0, column=0, padx=5, pady=5)
    tk.Entry(top, textvariable=name_var, font=("Arial", 12), width=25).grid(row=0, column=1, padx=5, pady=5)

    tk.Label(top, text="Password", font=("Arial", 12)).grid(row=1, column=0, padx=5, pady=5)
    tk.Entry(top, textvariable=p1_var, show="*", font=("Arial", 12), width=25).grid(row=1, column=1, padx=5, pady=5)

    tk.Label(top, text="Confirm Password", font=("Arial", 12)).grid(row=2, column=0, padx=5, pady=5)
    tk.Entry(top, textvariable=p2_var, show="*", font=("Arial", 12), width=25).grid(row=2, column=1, padx=5, pady=5)

    def do_register():
        """Handle register button click"""
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

    # Create register button
    tk.Button(top, text="Register", command=do_register, font=("Arial", 12), width=10).grid(row=3, column=0, columnspan=2, padx=5, pady=10)

# --- login window ---
root = tk.Tk()
root.title("Login")
root.geometry("320x250")
root.attributes("-topmost", True); root.after(150, lambda: root.attributes("-topmost", False))

# Set theme
sv_ttk.set_theme("dark")

# Create login form
tk.Label(root, text="Username:", font=("Arial", 12)).grid(row=0, column=0, padx=5, pady=5)
entry_username = tk.Entry(root, font=("Arial", 12), width=25)
entry_username.grid(row=0, column=1, padx=5, pady=5)

tk.Label(root, text="Password:", font=("Arial", 12)).grid(row=1, column=0, padx=5, pady=5)
entry_password = tk.Entry(root, show="*", font=("Arial", 12), width=25)
entry_password.grid(row=1, column=1, padx=5, pady=5)


# Create login and register buttons
btns = tk.Frame(root)
btns.grid(row=2, column=0, columnspan=2, padx=5, pady=10)

tk.Button(btns, text="Login", command=do_login, font=("Arial", 12), width=10, bg="#4CAF50", fg="#ffffff").pack(side=tk.LEFT, padx=5)
tk.Button(btns, text="Register", command=open_register, font=("Arial", 12), width=10, bg="#03A9F4", fg="#ffffff").pack(side=tk.LEFT, padx=5)

root.mainloop()
