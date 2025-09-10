import tkinter as tk
from tkinter import messagebox, filedialog,ttk
import backend

# add scroll bar so the contents will not be pushed outside the screen
class HScrollFrame(tk.Frame):
    def __init__(self, master, height=130, **kwargs):
        super().__init__(master, **kwargs)
        self.canvas = tk.Canvas(self, height=height, highlightthickness=0)
        self.hbar = tk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
        self.canvas.configure(xscrollcommand=self.hbar.set)

        self.canvas.pack(side="top", fill="x", expand=False)
        self.hbar.pack(side="top", fill="x")

        self.inner = tk.Frame(self.canvas)
        self.inner.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
import sv_ttk

# class MainInterface:
#     def __init__(self, role: str):
#         self.role = role  # "super" or "normal"
#         self.root = tk.Tk()
#         self.root.title("Clinic")
#         self.root.geometry("620x360")

#         title = f"Clinic ({'Admin' if self.role=='super' else 'Viewer'})"
#         tk.Label(self.root, text=title, font=("Georgia", 18, "bold")).pack(pady=12)

#         btns = tk.Frame(self.root)
#         btns.pack(pady=10)

#         tk.Button(btns, text="Patients",      width=18, command=self.open_patients).grid(     row=0, column=0, padx=6, pady=6)
#         tk.Button(btns, text="Doctors",       width=18, command=self.open_doctors).grid(      row=0, column=1, padx=6, pady=6)
#         tk.Button(btns, text="Appointments",  width=18, command=self.open_appointments).grid( row=1, column=0, padx=6, pady=6)
#         tk.Button(btns, text="Observations",  width=18, command=self.open_observations).grid( row=1, column=1, padx=6, pady=6)
#         tk.Button(btns, text="Diagnoses",     width=18, command=self.open_diagnoses).grid(    row=2, column=0, padx=6, pady=6)
#         tk.Button(btns, text="Clinics",       width=18, command=self.open_clinics).grid(      row=2, column=1, padx=6, pady=6)
#         tk.Button(btns, text="Departments",   width=18, command=self.open_departments).grid(  row=3, column=0, padx=6, pady=6)
#         # This is where the magic happens
#         # sv_ttk.set_theme("dark")
#         self.root.mainloop()

class MainInterface:
    def __init__(self, role: str):
        self.role = role  # "super" or "normal"
        self.root = tk.Tk()
        self.root.title("Clinic")
        self.root.geometry("800x600")
        self.root.config(bg="#f0f0f0")

        title = f"Clinic ({'Admin' if self.role=='super' else 'Viewer'})"
        tk.Label(self.root, text=title, font=("Georgia", 24, "bold"), bg="#f0f0f0", fg="#3498db").pack(pady=20)

        btns = tk.Frame(self.root, bg="#f0f0f0")
        btns.pack(pady=20)

        buttons = [
            {"text": "Patients", "command": self.open_patients},
            {"text": "Doctors", "command": self.open_doctors},
            {"text": "Appointments", "command": self.open_appointments},
            {"text": "Observations", "command": self.open_observations},
            {"text": "Diagnoses", "command": self.open_diagnoses},
            {"text": "Clinics", "command": self.open_clinics},
            {"text": "Departments", "command": self.open_departments},
        ]

        row_val = 0
        col_val = 0

        for button in buttons:
            tk.Button(btns, text=button["text"], width=15, height=2, font=("Georgia", 14), bg="#ecf0f1", fg="#2ecc71", command=button["command"]).grid(row=row_val, column=col_val, padx=10, pady=10)
            col_val += 1
            if col_val > 3:
                col_val = 0
                row_val += 1

        self.root.mainloop()


    # utilities 
    # keep HScrollFrame class from earlier in your file

    def _make_page(self, title: str, width=1000, height=640):
        win = tk.Toplevel(self.root)
        win.title(title)
        win.geometry(f"{width}x{height}")

        # --- Scrollable header (top) ---
        top_wrap = HScrollFrame(win, height=130)   # adjust height if you want more room
        top_wrap.pack(fill="x", padx=8, pady=6)
        top = top_wrap.inner  # use this as the parent for your grid() controls

        # --- Results area with BOTH scrollbars ---
        mid = tk.Frame(win)
        mid.pack(fill="both", expand=True, padx=8, pady=6)

        lb = tk.Listbox(mid, font=("Consolas", 10))
        ysb = tk.Scrollbar(mid, orient="vertical", command=lb.yview)
        xsb = tk.Scrollbar(mid, orient="horizontal", command=lb.xview)

        # wire the listbox to the scrollbars
        lb.configure(yscrollcommand=ysb.set, xscrollcommand=xsb.set)

        # layout: vertical bar on right, horizontal bar on bottom, listbox takes the rest
        ysb.pack(side="right", fill="y")
        xsb.pack(side="bottom", fill="x")
        lb.pack(side="left", fill="both", expand=True)

        return win, top, lb



    def _fill_with_headers(self, lb, headers, rows):
        lb.delete(0, tk.END)
        header_line = " | ".join(headers)
        lb.insert(tk.END, header_line)
        lb.insert(tk.END, "-" * len(header_line))
        if not rows:
            lb.insert(tk.END, "No results")
            return
        for r in rows:
            lb.insert(tk.END, " | ".join("" if x is None else str(x) for x in r))

    def _add(self, fn, lb, refresh_fn, headers):
        try:
            fn() # perform insert/delete/update
            # 
            self._fill_with_headers(lb, headers, refresh_fn()) #re-query and reprint the data
        except Exception as e:
            messagebox.showerror("DB error", str(e))

    # Patients
    def open_patients(self):
        win, top, lb = self._make_page("Patients")

        p_name = tk.StringVar(); p_num = tk.StringVar()
        d_name = tk.StringVar(); d_num = tk.StringVar()

        tk.Label(top, text="Patient name").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=p_name, width=18).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Patient personnumer").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=p_num, width=18).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Doctor name").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=d_name, width=18).grid(row=0, column=5, padx=4)
        tk.Label(top, text="Doctor personnumer").grid(row=0, column=6, padx=4)
        tk.Entry(top, textvariable=d_num, width=18).grid(row=0, column=7, padx=4)

        headers = [
            "patient_personnumer","patient_name",
            "doctor_personnumer","doctor_name"
        ]

        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(
                lb, headers, backend.patient_search(p_name.get(), p_num.get(), d_name.get(), d_num.get())
            )
        ).grid(row=0, column=8, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.patient_view())
        ).grid(row=0, column=9, padx=6)

        if self.role == "super":
            # Add 
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=12, sticky="we", pady=6)
            new_pn = tk.StringVar(); new_name = tk.StringVar(); new_doc = tk.StringVar()
            tk.Label(top, text="Add patient: personnumer").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=new_pn, width=18).grid(row=2, column=1, padx=4)
            tk.Label(top, text="full name").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=new_name, width=18).grid(row=2, column=3, padx=4)
            tk.Label(top, text="doctor personnumer").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=new_doc, width=18).grid(row=2, column=5, padx=4)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(
                    lambda: backend.patient_insert(new_pn.get(), new_name.get(), new_doc.get()),
                    lb, backend.patient_view, headers
                )
            ).grid(row=2, column=6, padx=6)

            # Delete
            del_pid = tk.StringVar()
            tk.Label(top, text="Delete patient personnumer").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_pid, width=18).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.patient_delete(del_pid.get()), lb, backend.patient_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update 
            up_pid = tk.StringVar(); up_pname = tk.StringVar(); up_docpn = tk.StringVar()
            tk.Label(top, text="Update by patient personnumer").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=up_pid, width=18).grid(row=4, column=1, padx=4)
            tk.Label(top, text="new full name").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=up_pname, width=18).grid(row=4, column=3, padx=4)
            tk.Label(top, text="new doctor personnumer").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=up_docpn, width=18).grid(row=4, column=5, padx=4)
            tk.Button(
                top, text="Update",
                command=lambda: self._add(
                    lambda: backend.patient_update(up_pid.get(), up_docpn.get(), up_pname.get()),
                    lb, backend.patient_view, headers
                )
            ).grid(row=4, column=6, padx=6)

        self._fill_with_headers(lb, headers, [])

    # Doctors 
    def open_doctors(self):
        win, top, lb = self._make_page("Doctors")

        d_name = tk.StringVar(); d_num = tk.StringVar()
        p_name = tk.StringVar(); p_num = tk.StringVar()

        tk.Label(top, text="Doctor name").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=d_name, width=18).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Doctor personnumer").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=d_num, width=18).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Patient name").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=p_name, width=18).grid(row=0, column=5, padx=4)
        tk.Label(top, text="Patient personnumer").grid(row=0, column=6, padx=4)
        tk.Entry(top, textvariable=p_num, width=18).grid(row=0, column=7, padx=4)

        headers = [
            "doctor_personnumer","doctor_name","dept_id",
            "patient_personnumer","patient_name"
        ]

        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(
                lb, headers,
                backend.doctor_search(d_name.get(), d_num.get(), p_name.get(), p_num.get())
                )
        ).grid(row=0, column=8, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.doctor_view())
        ).grid(row=0, column=9, padx=6)

        if self.role == "super":
            # Add 
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=12, sticky="we", pady=6)
            nd_num = tk.StringVar(); nd_name = tk.StringVar(); ndept = tk.StringVar()
            tk.Label(top, text="Add doctor: personnumer").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=nd_num, width=18).grid(row=2, column=1, padx=4)
            tk.Label(top, text="full name").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=nd_name, width=18).grid(row=2, column=3, padx=4)
            tk.Label(top, text="dept_id").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=ndept, width=14).grid(row=2, column=5, padx=4)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(
                    lambda: backend.doctor_insert(nd_num.get(), nd_name.get(), ndept.get()),
                    lb, backend.doctor_view, headers
                )
            ).grid(row=2, column=6, padx=6)

            # Delete
            del_did = tk.StringVar()
            tk.Label(top, text="Delete doctor personnumer").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_did, width=18).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.doctor_delete(del_did.get()), lb, backend.doctor_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update
            up_did = tk.StringVar(); up_dname = tk.StringVar(); up_dept = tk.StringVar()
            tk.Label(top, text="Update by doctor personnumer").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=up_did, width=18).grid(row=4, column=1, padx=4)
            tk.Label(top, text="new full name").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=up_dname, width=18).grid(row=4, column=3, padx=4)
            tk.Label(top, text="new dept_id").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=up_dept, width=14).grid(row=4, column=5, padx=4)
            tk.Button(
                top, text="Update",
                command=lambda: self._add(
                    lambda: backend.doctor_update(up_did.get(), up_dept.get(), up_dname.get()),
                    lb, backend.doctor_view, headers
                )
            ).grid(row=4, column=6, padx=6)

        self._fill_with_headers(lb, headers, [])

    # Appointments 
    def open_appointments(self):
        win, top, lb = self._make_page("Appointments")

        ap_id = tk.StringVar(); y = tk.StringVar(); m = tk.StringVar(); d = tk.StringVar()
        p_name = tk.StringVar(); p_num = tk.StringVar()
        doc_name = tk.StringVar(); doc_num = tk.StringVar()

        tk.Label(top, text="ID").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=ap_id, width=12).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Year").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=y, width=6).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Month").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=m, width=6).grid(row=0, column=5, padx=4)
        tk.Label(top, text="Day").grid(row=0, column=6, padx=4)
        tk.Entry(top, textvariable=d, width=6).grid(row=0, column=7, padx=4)
        tk.Label(top, text="Patient name").grid(row=0, column=8, padx=4)
        tk.Entry(top, textvariable=p_name, width=16).grid(row=0, column=9, padx=4)
        tk.Label(top, text="Patient personnumer").grid(row=0, column=10, padx=4)
        tk.Entry(top, textvariable=p_num, width=16).grid(row=0, column=11, padx=4)
        tk.Label(top, text="Doctor name").grid(row=0, column=12, padx=4)
        tk.Entry(top, textvariable=doc_name, width=16).grid(row=0, column=13, padx=4)
        tk.Label(top, text="Doctor personnumer").grid(row=0, column=14, padx=4)
        tk.Entry(top, textvariable=doc_num, width=16).grid(row=0, column=15, padx=4)

        headers = [
            "appoint_id","year","month","day","location",
            "patient_personnumer","patient_name","doctor_personnumer","doctor_name"
        ]

        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(
                lb, headers,
                backend.appointment_search(ap_id.get(), y.get(), m.get(), d.get(),
                                           p_name.get(), p_num.get(), doc_name.get(), doc_num.get())
            )
        ).grid(row=0, column=16, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.appointment_view())
        ).grid(row=0, column=17, padx=6)

        if self.role == "super":
            # Add
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=18, sticky="we", pady=6)
            na_id = tk.StringVar(); ny = tk.StringVar(); nm = tk.StringVar(); nd = tk.StringVar()
            nloc = tk.StringVar(); np = tk.StringVar(); ndoc = tk.StringVar()
            tk.Label(top, text="Add: id").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=na_id, width=12).grid(row=2, column=1, padx=4)
            tk.Label(top, text="year").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=ny, width=6).grid(row=2, column=3, padx=4)
            tk.Label(top, text="month").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=nm, width=6).grid(row=2, column=5, padx=4)
            tk.Label(top, text="day").grid(row=2, column=6, padx=4)
            tk.Entry(top, textvariable=nd, width=6).grid(row=2, column=7, padx=4)
            tk.Label(top, text="location").grid(row=2, column=8, padx=4)
            tk.Entry(top, textvariable=nloc, width=16).grid(row=2, column=9, padx=4)
            tk.Label(top, text="patient personnumer").grid(row=2, column=10, padx=4)
            tk.Entry(top, textvariable=np, width=16).grid(row=2, column=11, padx=4)
            tk.Label(top, text="doctor personnumer").grid(row=2, column=12, padx=4)
            tk.Entry(top, textvariable=ndoc, width=16).grid(row=2, column=13, padx=4)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(
                    lambda: backend.appointment_insert(na_id.get(), ny.get(), nm.get(), nd.get(), nloc.get(), np.get(), ndoc.get()),
                    lb, backend.appointment_view, headers
                )
            ).grid(row=2, column=14, padx=6)

            # Delete
            del_aid = tk.StringVar()
            tk.Label(top, text="Delete appoint_id").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_aid, width=12).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.appointment_delete(del_aid.get()), lb, backend.appointment_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update
            up_aid = tk.StringVar(); uy = tk.StringVar(); um = tk.StringVar(); ud = tk.StringVar()
            uloc = tk.StringVar(); upn = tk.StringVar(); udn = tk.StringVar()
            tk.Label(top, text="Update appoint_id").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=up_aid, width=12).grid(row=4, column=1, padx=4)
            tk.Label(top, text="year").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=uy, width=6).grid(row=4, column=3, padx=4)
            tk.Label(top, text="month").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=um, width=6).grid(row=4, column=5, padx=4)
            tk.Label(top, text="day").grid(row=4, column=6, padx=4)
            tk.Entry(top, textvariable=ud, width=6).grid(row=4, column=7, padx=4)
            tk.Label(top, text="location").grid(row=4, column=8, padx=4)
            tk.Entry(top, textvariable=uloc, width=16).grid(row=4, column=9, padx=4)
            tk.Label(top, text="patient personnumer").grid(row=4, column=10, padx=4)
            tk.Entry(top, textvariable=upn, width=16).grid(row=4, column=11, padx=4)
            tk.Label(top, text="doctor personnumer").grid(row=4, column=12, padx=4)
            tk.Entry(top, textvariable=udn, width=16).grid(row=4, column=13, padx=4)
            tk.Button(
                top, text="Update",
                command=lambda: self._add(
                    lambda: backend.appointment_update(up_aid.get(), uy.get(), um.get(), ud.get(),
                                                       uloc.get(), upn.get(), udn.get()),
                    lb, backend.appointment_view, headers
                )
            ).grid(row=4, column=14, padx=6)

        self._fill_with_headers(lb, headers, [])

    # Observations
    def open_observations(self):
        win, top, lb = self._make_page("Observations")

        obs_id = tk.StringVar(); y = tk.StringVar(); m = tk.StringVar(); d = tk.StringVar(); ap_id = tk.StringVar()
        p_name = tk.StringVar(); p_num = tk.StringVar()
        doc_name = tk.StringVar(); doc_num = tk.StringVar()

        tk.Label(top, text="Obs ID").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=obs_id, width=12).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Year").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=y, width=6).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Month").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=m, width=6).grid(row=0, column=5, padx=4)
        tk.Label(top, text="Day").grid(row=0, column=6, padx=4)
        tk.Entry(top, textvariable=d, width=6).grid(row=0, column=7, padx=4)
        tk.Label(top, text="Appt ID").grid(row=0, column=8, padx=4)
        tk.Entry(top, textvariable=ap_id, width=12).grid(row=0, column=9, padx=4)
        tk.Label(top, text="Patient name").grid(row=0, column=10, padx=4)
        tk.Entry(top, textvariable=p_name, width=16).grid(row=0, column=11, padx=4)
        tk.Label(top, text="Patient personnumer").grid(row=0, column=12, padx=4)
        tk.Entry(top, textvariable=p_num, width=16).grid(row=0, column=13, padx=4)
        tk.Label(top, text="Doctor name").grid(row=0, column=14, padx=4)
        tk.Entry(top, textvariable=doc_name, width=16).grid(row=0, column=15, padx=4)
        tk.Label(top, text="Doctor personnumer").grid(row=0, column=16, padx=4)
        tk.Entry(top, textvariable=doc_num, width=16).grid(row=0, column=17, padx=4)

        # headers (Observations)
        headers = [
            "obser_id","year","month","day","appoint_id",
            "patient_personnumer","patient_name",
            "doctor_personnumer","doctor_name",
            "comment_text", "file_oid"
        ]


        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(
                lb, headers,
                backend.observation_search(obs_id.get(), y.get(), m.get(), d.get(), ap_id.get(),
                                           p_name.get(), p_num.get(), doc_name.get(), doc_num.get())
            )
        ).grid(row=0, column=18, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.observation_view())
        ).grid(row=0, column=19, padx=6)

        if self.role == "super":
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=20, sticky="we", pady=6)
            # Add 
            no_id = tk.StringVar(); ny = tk.StringVar(); nm = tk.StringVar(); nd = tk.StringVar()
            napid = tk.StringVar()
            comment_text = tk.StringVar(); file_path = tk.StringVar()

            tk.Label(top, text="Add: obser_id").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=no_id, width=12).grid(row=2, column=1, padx=4)
            tk.Label(top, text="year").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=ny, width=6).grid(row=2, column=3, padx=4)
            tk.Label(top, text="month").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=nm, width=6).grid(row=2, column=5, padx=4)
            tk.Label(top, text="day").grid(row=2, column=6, padx=4)
            tk.Entry(top, textvariable=nd, width=6).grid(row=2, column=7, padx=4)
            tk.Label(top, text="appoint_id").grid(row=2, column=8, padx=4)
            tk.Entry(top, textvariable=napid, width=12).grid(row=2, column=9, padx=4)

            tk.Label(top, text="text comment").grid(row=2, column=10, padx=4)
            tk.Entry(top, textvariable=comment_text, width=24).grid(row=2, column=11, padx=4, columnspan=2)

            def pick_file_add():
                p = filedialog.askopenfilename(title="Pick image/video/text", filetypes=[("All", "*.*")])
                if p:
                    file_path.set(p)
            tk.Button(top, text="Choose file", command=pick_file_add).grid(row=2, column=13, padx=6)
            tk.Label(top, textvariable=file_path, fg="gray").grid(row=2, column=14, columnspan=4, sticky="w")

            def do_add_obs():
                comment_txt = (comment_text.get().strip() or None)
                file_oid = None
                fp = file_path.get().strip()
                if fp:
                    file_oid = backend.lo_save_file(fp)   # create LO and get OID
                backend.observation_insert(no_id.get(), ny.get(), nm.get(), nd.get(),
                                        (napid.get() or None),
                                        comment_txt, file_oid)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(do_add_obs, lb, backend.observation_view, headers)
            ).grid(row=2, column=18, padx=6)

            # Delete
            del_oid = tk.StringVar()
            tk.Label(top, text="Delete obser_id").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_oid, width=12).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.observation_delete(del_oid.get()), lb, backend.observation_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update
            uo_id = tk.StringVar(); uy = tk.StringVar(); um = tk.StringVar(); ud = tk.StringVar()
            uapid = tk.StringVar(); u_comment_text = tk.StringVar(); u_file_path = tk.StringVar()

            tk.Label(top, text="Update: obser_id").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=uo_id, width=12).grid(row=4, column=1, padx=4)
            tk.Label(top, text="year").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=uy, width=6).grid(row=4, column=3, padx=4)
            tk.Label(top, text="month").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=um, width=6).grid(row=4, column=5, padx=4)
            tk.Label(top, text="day").grid(row=4, column=6, padx=4)
            tk.Entry(top, textvariable=ud, width=6).grid(row=4, column=7, padx=4)
            tk.Label(top, text="appoint_id").grid(row=4, column=8, padx=4)
            tk.Entry(top, textvariable=uapid, width=12).grid(row=4, column=9, padx=4)

            tk.Label(top, text="new text comment").grid(row=4, column=10, padx=4)
            tk.Entry(top, textvariable=u_comment_text, width=24).grid(row=4, column=11, padx=4, columnspan=2)

            def pick_file_update():
                p = filedialog.askopenfilename(title="Pick image/video/text", filetypes=[("All", "*.*")])
                if p:
                    u_file_path.set(p)
            tk.Button(top, text="Choose file", command=pick_file_update).grid(row=4, column=13, padx=6)
            tk.Label(top, textvariable=u_file_path, fg="gray").grid(row=4, column=14, columnspan=4, sticky="w")

            def do_update_obs():
                new_comment_txt = (u_comment_text.get().strip() if u_comment_text.get().strip() != "" else None)
                new_file_oid = None
                fp = u_file_path.get().strip()
                if fp:
                    new_file_oid = backend.lo_save_file(fp)
                backend.observation_update(uo_id.get(), uy.get(), um.get(), ud.get(),
                                        (uapid.get() or None),
                                        new_comment_txt, new_file_oid)

            tk.Button(
                top, text="Update",
                command=lambda: self._add(do_update_obs, lb, backend.observation_view, headers)
            ).grid(row=4, column=18, padx=6)

        self._fill_with_headers(lb, headers, [])

    # Diagnoses
    def open_diagnoses(self):
        win, top, lb = self._make_page("Diagnoses")

        dg_id = tk.StringVar(); y = tk.StringVar(); m = tk.StringVar(); d = tk.StringVar()
        obs_id = tk.StringVar(); ap_id = tk.StringVar()
        p_name = tk.StringVar(); p_num = tk.StringVar()
        doc_name = tk.StringVar(); doc_num = tk.StringVar()

        tk.Label(top, text="Diagn ID").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=dg_id, width=12).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Year").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=y, width=6).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Month").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=m, width=6).grid(row=0, column=5, padx=4)
        tk.Label(top, text="Day").grid(row=0, column=6, padx=4)
        tk.Entry(top, textvariable=d, width=6).grid(row=0, column=7, padx=4)

        tk.Label(top, text="Obs ID").grid(row=0, column=8, padx=4)
        tk.Entry(top, textvariable=obs_id, width=12).grid(row=0, column=9, padx=4)
        tk.Label(top, text="Appt ID").grid(row=0, column=10, padx=4)
        tk.Entry(top, textvariable=ap_id, width=12).grid(row=0, column=11, padx=4)

        tk.Label(top, text="Patient name").grid(row=0, column=12, padx=4)
        tk.Entry(top, textvariable=p_name, width=16).grid(row=0, column=13, padx=4)
        tk.Label(top, text="Patient personnumer").grid(row=0, column=14, padx=4)
        tk.Entry(top, textvariable=p_num, width=16).grid(row=0, column=15, padx=4)
        tk.Label(top, text="Doctor name").grid(row=0, column=16, padx=4)
        tk.Entry(top, textvariable=doc_name, width=16).grid(row=0, column=17, padx=4)
        tk.Label(top, text="Doctor personnumer").grid(row=0, column=18, padx=4)
        tk.Entry(top, textvariable=doc_num, width=16).grid(row=0, column=19, padx=4)

        headers = [
            "diagn_id","year","month","day","obser_id","appoint_id",
            "patient_personnumer","patient_name","doctor_personnumer","doctor_name",
            "comment_text","file_oid"
        ]

        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(
                lb, headers,
                backend.diagnosis_search(dg_id.get(), y.get(), m.get(), d.get(),
                                         obs_id.get(), ap_id.get(),
                                         p_name.get(), p_num.get(),
                                         doc_name.get(), doc_num.get())
            )
        ).grid(row=0, column=20, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.diagnosis_view())
        ).grid(row=0, column=21, padx=6)

        if self.role == "super":
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=22, sticky="we", pady=6)
            # Add
            ndg_id = tk.StringVar(); ny = tk.StringVar(); nm = tk.StringVar(); nd = tk.StringVar()
            nobs_id = tk.StringVar()
            comment_text = tk.StringVar(); file_path = tk.StringVar()

            tk.Label(top, text="Add: diagn_id").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=ndg_id, width=12).grid(row=2, column=1, padx=4)
            tk.Label(top, text="year").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=ny, width=6).grid(row=2, column=3, padx=4)
            tk.Label(top, text="month").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=nm, width=6).grid(row=2, column=5, padx=4)
            tk.Label(top, text="day").grid(row=2, column=6, padx=4)
            tk.Entry(top, textvariable=nd, width=6).grid(row=2, column=7, padx=4)
            tk.Label(top, text="obser_id").grid(row=2, column=8, padx=4)
            tk.Entry(top, textvariable=nobs_id, width=12).grid(row=2, column=9, padx=4)

            tk.Label(top, text="text comment").grid(row=2, column=10, padx=4)
            tk.Entry(top, textvariable=comment_text, width=24).grid(row=2, column=11, padx=4, columnspan=2)

            def pick_file_add():
                p = filedialog.askopenfilename(title="Pick image/video/text", filetypes=[("All", "*.*")])
                if p:
                    file_path.set(p)
            tk.Button(top, text="Choose file", command=pick_file_add).grid(row=2, column=13, padx=6)
            tk.Label(top, textvariable=file_path, fg="gray").grid(row=2, column=14, columnspan=5, sticky="w")

            def do_add_diagn():
                comment_txt = (comment_text.get().strip() or None)
                file_oid = None
                fp = file_path.get().strip()
                if fp:
                    file_oid = backend.lo_save_file(fp)
                backend.diagnosis_insert(ndg_id.get(), ny.get(), nm.get(), nd.get(),
                                        (nobs_id.get() or None),
                                        comment_txt, file_oid)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(do_add_diagn, lb, backend.diagnosis_view, headers)
            ).grid(row=2, column=20, padx=6)

            # Delete
            del_dgid = tk.StringVar()
            tk.Label(top, text="Delete diagn_id").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_dgid, width=12).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.diagnosis_delete(del_dgid.get()), lb, backend.diagnosis_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update
            udg_id = tk.StringVar(); uy = tk.StringVar(); um = tk.StringVar(); ud = tk.StringVar()
            uobs_id = tk.StringVar(); u_comment_text = tk.StringVar(); u_file_path = tk.StringVar()

            tk.Label(top, text="Update: diagn_id").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=udg_id, width=12).grid(row=4, column=1, padx=4)
            tk.Label(top, text="year").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=uy, width=6).grid(row=4, column=3, padx=4)
            tk.Label(top, text="month").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=um, width=6).grid(row=4, column=5, padx=4)
            tk.Label(top, text="day").grid(row=4, column=6, padx=4)
            tk.Entry(top, textvariable=ud, width=6).grid(row=4, column=7, padx=4)
            tk.Label(top, text="obser_id").grid(row=4, column=8, padx=4)
            tk.Entry(top, textvariable=uobs_id, width=12).grid(row=4, column=9, padx=4)

            tk.Label(top, text="new text comment").grid(row=4, column=10, padx=4)
            tk.Entry(top, textvariable=u_comment_text, width=24).grid(row=4, column=11, padx=4, columnspan=2)

            def pick_file_update():
                p = filedialog.askopenfilename(title="Pick image/video/text", filetypes=[("All", "*.*")])
                if p:
                    u_file_path.set(p)
            tk.Button(top, text="Choose file", command=pick_file_update).grid(row=4, column=13, padx=6)
            tk.Label(top, textvariable=u_file_path, fg="gray").grid(row=4, column=14, columnspan=5, sticky="w")

            def do_update_diagn():
                new_comment_txt = (u_comment_text.get().strip() if u_comment_text.get().strip() != "" else None)
                new_file_oid = None
                fp = u_file_path.get().strip()
                if fp:
                    new_file_oid = backend.lo_save_file(fp)
                backend.diagnosis_update(udg_id.get(), uy.get(), um.get(), ud.get(),
                                        (uobs_id.get() or None),
                                        new_comment_txt, new_file_oid)

            tk.Button(
                top, text="Update",
                command=lambda: self._add(do_update_diagn, lb, backend.diagnosis_view, headers)
            ).grid(row=4, column=20, padx=6)

        self._fill_with_headers(lb, headers, [])

    # Clinics
    def open_clinics(self):
        win, top, lb = self._make_page("Clinics")

        cid = tk.StringVar(); name = tk.StringVar(); addr = tk.StringVar()

        tk.Label(top, text="Clinic ID").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=cid, width=16).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Name").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=name, width=18).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Address").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=addr, width=24).grid(row=0, column=5, padx=4)

        headers = ["cli_id","cli_name","address"]

        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(lb, headers, backend.clinic_search(cid.get(), name.get(), addr.get()))
        ).grid(row=0, column=6, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.clinic_view())
        ).grid(row=0, column=7, padx=6)

        if self.role == "super":
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=8, sticky="we", pady=6)
            # Add
            nid = tk.StringVar(); nname = tk.StringVar(); naddr = tk.StringVar()
            tk.Label(top, text="Add: cli_id").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=nid, width=16).grid(row=2, column=1, padx=4)
            tk.Label(top, text="name").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=nname, width=18).grid(row=2, column=3, padx=4)
            tk.Label(top, text="address").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=naddr, width=24).grid(row=2, column=5, padx=4)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(lambda: backend.clinic_insert(nid.get(), nname.get(), naddr.get()),
                                          lb, backend.clinic_view, headers)
            ).grid(row=2, column=6, padx=6)

            # Delete
            del_cid = tk.StringVar()
            tk.Label(top, text="Delete cli_id").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_cid, width=16).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.clinic_delete(del_cid.get()), lb, backend.clinic_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update
            up_cid = tk.StringVar(); up_cname = tk.StringVar(); up_caddr = tk.StringVar()
            tk.Label(top, text="Update cli_id").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=up_cid, width=16).grid(row=4, column=1, padx=4)
            tk.Label(top, text="new name").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=up_cname, width=18).grid(row=4, column=3, padx=4)
            tk.Label(top, text="new address").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=up_caddr, width=24).grid(row=4, column=5, padx=4)
            tk.Button(
                top, text="Update",
                command=lambda: self._add(lambda: backend.clinic_update(up_cid.get(), up_cname.get(), up_caddr.get()),
                                          lb, backend.clinic_view, headers)
            ).grid(row=4, column=6, padx=6)

        self._fill_with_headers(lb, headers, [])

    # Departments 
    def open_departments(self):
        win, top, lb = self._make_page("Departments")

        did = tk.StringVar(); dname = tk.StringVar(); cid = tk.StringVar(); cname = tk.StringVar()

        tk.Label(top, text="Dept ID").grid(row=0, column=0, padx=4)
        tk.Entry(top, textvariable=did, width=16).grid(row=0, column=1, padx=4)
        tk.Label(top, text="Dept name").grid(row=0, column=2, padx=4)
        tk.Entry(top, textvariable=dname, width=18).grid(row=0, column=3, padx=4)
        tk.Label(top, text="Clinic ID").grid(row=0, column=4, padx=4)
        tk.Entry(top, textvariable=cid, width=16).grid(row=0, column=5, padx=4)
        tk.Label(top, text="Clinic name").grid(row=0, column=6, padx=4)
        tk.Entry(top, textvariable=cname, width=18).grid(row=0, column=7, padx=4)

        headers = ["dept_id","dept_name","cli_id","clinic_name"]

        tk.Button(
            top, text="Search",
            command=lambda: self._fill_with_headers(lb, headers, backend.department_search(did.get(), dname.get(), cid.get(), cname.get()))
        ).grid(row=0, column=8, padx=6)
        tk.Button(
            top, text="View All",
            command=lambda: self._fill_with_headers(lb, headers, backend.department_view())
        ).grid(row=0, column=9, padx=6)

        if self.role == "super":
            sep = tk.Frame(top, height=2, bd=1, relief="sunken"); sep.grid(row=1, column=0, columnspan=10, sticky="we", pady=6)
            # Add
            ndid = tk.StringVar(); ndname = tk.StringVar(); ncid = tk.StringVar()
            tk.Label(top, text="Add: dept_id").grid(row=2, column=0, padx=4)
            tk.Entry(top, textvariable=ndid, width=16).grid(row=2, column=1, padx=4)
            tk.Label(top, text="dept_name").grid(row=2, column=2, padx=4)
            tk.Entry(top, textvariable=ndname, width=18).grid(row=2, column=3, padx=4)
            tk.Label(top, text="cli_id").grid(row=2, column=4, padx=4)
            tk.Entry(top, textvariable=ncid, width=16).grid(row=2, column=5, padx=4)
            tk.Button(
                top, text="Add",
                command=lambda: self._add(lambda: backend.department_insert(ndid.get(), ndname.get(), ncid.get()),
                                          lb, backend.department_view, headers)
            ).grid(row=2, column=6, padx=6)

            # Delete 
            del_dpid = tk.StringVar()
            tk.Label(top, text="Delete dept_id").grid(row=3, column=0, padx=4)
            tk.Entry(top, textvariable=del_dpid, width=16).grid(row=3, column=1, padx=4)
            tk.Button(
                top, text="Delete",
                command=lambda: self._add(lambda: backend.department_delete(del_dpid.get()), lb, backend.department_view, headers)
            ).grid(row=3, column=2, padx=6)

            # Update 
            up_dpid = tk.StringVar(); up_dname = tk.StringVar(); up_cid = tk.StringVar()
            tk.Label(top, text="Update dept_id").grid(row=4, column=0, padx=4)
            tk.Entry(top, textvariable=up_dpid, width=16).grid(row=4, column=1, padx=4)
            tk.Label(top, text="new dept_name").grid(row=4, column=2, padx=4)
            tk.Entry(top, textvariable=up_dname, width=18).grid(row=4, column=3, padx=4)
            tk.Label(top, text="new cli_id").grid(row=4, column=4, padx=4)
            tk.Entry(top, textvariable=up_cid, width=16).grid(row=4, column=5, padx=4)
            tk.Button(
                top, text="Update",
                command=lambda: self._add(lambda: backend.department_update(up_dpid.get(), up_dname.get(), up_cid.get()),
                                          lb, backend.department_view, headers)
            ).grid(row=4, column=6, padx=6)

        self._fill_with_headers(lb, headers, [])
