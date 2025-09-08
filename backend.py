from db import get_conn

def _to_int(x):
    x = (x or "").strip()
    return int(x) if x != "" else None

def _is_blank(x) -> bool:
    return x is None or (isinstance(x, str) and x.strip() == "")

# PERSON 
def person_search(q: str):
    q = (q or "").strip()
    with get_conn() as (conn, cur):
        if not q:
            cur.execute("""
                SELECT personnumer, full_name
                FROM person
                ORDER BY personnumer;
            """)
        else:
            cur.execute("""
                SELECT personnumer, full_name
                FROM person
                WHERE personnumer ILIKE %s OR full_name ILIKE %s
                ORDER BY personnumer;
            """, (f"%{q}%", f"%{q}%"))
        return cur.fetchall()

def person_insert(personnumer: str, full_name: str):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO person (personnumer, full_name)
            VALUES (%s, %s)
            ON CONFLICT (personnumer) DO NOTHING;
        """, (personnumer, full_name))
        conn.commit()

def person_update_name(personnumer: str, full_name: str | None):
    if _is_blank(full_name):
        return
    with get_conn() as (conn, cur):
        cur.execute("UPDATE person SET full_name=%s WHERE personnumer=%s", (full_name, personnumer))
        conn.commit()

#  PATIENTS
def patient_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT pa.patient_id,
                   pe.personnumer   AS patient_personnumer,
                   pe.full_name     AS patient_name,
                   pa.doctor_personnumer,
                   d.doctor_id,
                   dp.full_name     AS doctor_name
            FROM patient pa
            JOIN person pe ON pe.personnumer = pa.personnumer
            LEFT JOIN doctor d ON d.personnumer = pa.doctor_personnumer
            LEFT JOIN person dp ON dp.personnumer = d.personnumer
            ORDER BY pa.patient_id;
        """)
        return cur.fetchall()

def patient_search(patient_name="", patient_personnumer="", doctor_name="", doctor_personnumer=""):
    where, args = [], []
    if patient_name:
        where.append("pe.full_name ILIKE %s");           args.append(f"%{patient_name}%")
    if patient_personnumer:
        where.append("pe.personnumer ILIKE %s");         args.append(f"%{patient_personnumer}%")
    if doctor_name:
        where.append("dp.full_name ILIKE %s");           args.append(f"%{doctor_name}%")
    if doctor_personnumer:
        where.append("pa.doctor_personnumer ILIKE %s");  args.append(f"%{doctor_personnumer}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT pa.patient_id,
                   pe.personnumer   AS patient_personnumer,
                   pe.full_name     AS patient_name,
                   pa.doctor_personnumer,
                   d.doctor_id,
                   dp.full_name     AS doctor_name
            FROM patient pa
            JOIN person pe ON pe.personnumer = pa.personnumer
            LEFT JOIN doctor d ON d.personnumer = pa.doctor_personnumer
            LEFT JOIN person dp ON dp.personnumer = d.personnumer
            WHERE {clause}
            ORDER BY pa.patient_id;
        """, tuple(args))
        return cur.fetchall()

def patient_insert(patient_personnumer: str, patient_name: str, patient_id: str, doctor_personnumer: str | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO person (personnumer, full_name)
            VALUES (%s, %s)
            ON CONFLICT (personnumer) DO NOTHING;
        """, (patient_personnumer, patient_name))
        cur.execute("""
            INSERT INTO patient (personnumer, patient_id, doctor_personnumer)
            VALUES (%s, %s, %s)
        """, (patient_personnumer, patient_id, (doctor_personnumer or None)))
        conn.commit()

def patient_update(patient_id: str, new_doctor_personnumer: str | None = None, new_patient_name: str | None = None):
    # Update doctor link
    with get_conn() as (conn, cur):
        if not _is_blank(new_doctor_personnumer):
            cur.execute("UPDATE patient SET doctor_personnumer=%s WHERE patient_id=%s",
                        (new_doctor_personnumer, patient_id))
        if not _is_blank(new_patient_name):
            # Update person.name via patient -> personnumer
            cur.execute("""
                UPDATE person SET full_name=%s
                WHERE personnumer=(SELECT personnumer FROM patient WHERE patient_id=%s)
            """, (new_patient_name, patient_id))
        conn.commit()

def patient_delete(patient_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM patient WHERE patient_id=%s", (patient_id,))
        conn.commit()

#  DOCTORS 
def doctor_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT DISTINCT
                   d.personnumer          AS doctor_personnumer,
                   d.doctor_id,
                   dp.full_name           AS doctor_name,
                   d.dept_id,
                   p.personnumer          AS patient_personnumer,
                   pp.full_name           AS patient_name
            FROM doctor d
            JOIN person dp ON dp.personnumer = d.personnumer
            LEFT JOIN patient p ON p.doctor_personnumer = d.personnumer
            LEFT JOIN person pp ON pp.personnumer = p.personnumer
            ORDER BY d.doctor_id;
        """)
        return cur.fetchall()

def doctor_search(doctor_name="", doctor_personnumer="", doctor_id="", patient_name="", patient_personnumer=""):
    where, args = [], []
    if doctor_name:
        where.append("dp.full_name ILIKE %s");           args.append(f"%{doctor_name}%")
    if doctor_personnumer:
        where.append("d.personnumer ILIKE %s");          args.append(f"%{doctor_personnumer}%")
    if doctor_id:
        where.append("d.doctor_id ILIKE %s");            args.append(f"%{doctor_id}%")
    if patient_name:
        where.append("pp.full_name ILIKE %s");           args.append(f"%{patient_name}%")
    if patient_personnumer:
        where.append("p.personnumer ILIKE %s");          args.append(f"%{patient_personnumer}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT DISTINCT
                   d.personnumer          AS doctor_personnumer,
                   d.doctor_id,
                   dp.full_name           AS doctor_name,
                   d.dept_id,
                   p.personnumer          AS patient_personnumer,
                   pp.full_name           AS patient_name
            FROM doctor d
            JOIN person dp ON dp.personnumer = d.personnumer
            LEFT JOIN patient p ON p.doctor_personnumer = d.personnumer
            LEFT JOIN person pp ON pp.personnumer = p.personnumer
            WHERE {clause}
            ORDER BY d.doctor_id;
        """, tuple(args))
        return cur.fetchall()

def doctor_insert(doctor_personnumer: str, doctor_name: str, doctor_id: str, dept_id: str | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO person (personnumer, full_name)
            VALUES (%s, %s)
            ON CONFLICT (personnumer) DO NOTHING;
        """, (doctor_personnumer, doctor_name))
        cur.execute("""
            INSERT INTO doctor (personnumer, doctor_id, dept_id)
            VALUES (%s, %s, %s)
        """, (doctor_personnumer, doctor_id, (dept_id or None)))
        conn.commit()

def doctor_update(doctor_id: str, new_dept_id: str | None = None, new_doctor_name: str | None = None):
    with get_conn() as (conn, cur):
        if not _is_blank(new_dept_id):
            cur.execute("UPDATE doctor SET dept_id=%s WHERE doctor_id=%s", (new_dept_id, doctor_id))
        if not _is_blank(new_doctor_name):
            cur.execute("""
                UPDATE person SET full_name=%s
                WHERE personnumer=(SELECT personnumer FROM doctor WHERE doctor_id=%s)
            """, (new_doctor_name, doctor_id))
        conn.commit()

def doctor_delete(doctor_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM doctor WHERE doctor_id=%s", (doctor_id,))
        conn.commit()

#  APPOINTMENTS 
def appointment_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT a.appoint_id, a.appoint_year, a.appoint_month, a.appoint_day, a.appoint_location,
                   p.personnumer AS patient_personnumer, pp.full_name AS patient_name,
                   d.personnumer AS doctor_personnumer, dp.full_name AS doctor_name
            FROM appointment a
            LEFT JOIN patient p ON p.personnumer = a.patient_personnumer
            LEFT JOIN person  pp ON pp.personnumer = p.personnumer
            LEFT JOIN doctor  d  ON d.personnumer  = a.doctor_personnumer
            LEFT JOIN person  dp ON dp.personnumer = d.personnumer
            ORDER BY a.appoint_year NULLS LAST, a.appoint_month NULLS LAST, a.appoint_day NULLS LAST, a.appoint_id;
        """)
        return cur.fetchall()

def appointment_search(appoint_id="", year="", month="", day="",
                       patient_name="", patient_personnumer="",
                       doctor_name="", doctor_personnumer=""):
    where, args = [], []
    if appoint_id:
        where.append("a.appoint_id ILIKE %s");     args.append(f"%{appoint_id}%")
    if year:
        where.append("a.appoint_year = %s");       args.append(_to_int(year))
    if month:
        where.append("a.appoint_month = %s");      args.append(_to_int(month))
    if day:
        where.append("a.appoint_day = %s");        args.append(_to_int(day))
    if patient_name:
        where.append("pp.full_name ILIKE %s");     args.append(f"%{patient_name}%")
    if patient_personnumer:
        where.append("p.personnumer ILIKE %s");    args.append(f"%{patient_personnumer}%")
    if doctor_name:
        where.append("dp.full_name ILIKE %s");     args.append(f"%{doctor_name}%")
    if doctor_personnumer:
        where.append("d.personnumer ILIKE %s");    args.append(f"%{doctor_personnumer}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT a.appoint_id, a.appoint_year, a.appoint_month, a.appoint_day, a.appoint_location,
                   p.personnumer AS patient_personnumer, pp.full_name AS patient_name,
                   d.personnumer AS doctor_personnumer, dp.full_name AS doctor_name
            FROM appointment a
            LEFT JOIN patient p ON p.personnumer = a.patient_personnumer
            LEFT JOIN person  pp ON pp.personnumer = p.personnumer
            LEFT JOIN doctor  d  ON d.personnumer  = a.doctor_personnumer
            LEFT JOIN person  dp ON dp.personnumer = d.personnumer
            WHERE {clause}
            ORDER BY a.appoint_year NULLS LAST, a.appoint_month NULLS LAST, a.appoint_day NULLS LAST, a.appoint_id;
        """, tuple(args))
        return cur.fetchall()

def appointment_insert(appoint_id: str, year: str, month: str, day: str,
                       location: str, patient_personnumer: str | None, doctor_personnumer: str | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO appointment (appoint_id, appoint_year, appoint_month, appoint_day,
                                     appoint_location, patient_personnumer, doctor_personnumer)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            appoint_id,
            _to_int(year), _to_int(month), _to_int(day),
            (location or None),
            (patient_personnumer or None),
            (doctor_personnumer or None)
        ))
        conn.commit()

def appointment_update(appoint_id: str, year: str | None = None, month: str | None = None, day: str | None = None,
                       location: str | None = None, patient_personnumer: str | None = None, doctor_personnumer: str | None = None):
    sets, args = [], []
    if not _is_blank(year):    sets.append("appoint_year=%s");  args.append(_to_int(year))
    if not _is_blank(month):   sets.append("appoint_month=%s"); args.append(_to_int(month))
    if not _is_blank(day):     sets.append("appoint_day=%s");   args.append(_to_int(day))
    if not _is_blank(location):sets.append("appoint_location=%s"); args.append(location)
    if not _is_blank(patient_personnumer): sets.append("patient_personnumer=%s"); args.append(patient_personnumer)
    if not _is_blank(doctor_personnumer):  sets.append("doctor_personnumer=%s");  args.append(doctor_personnumer)
    if not sets:
        return
    args.append(appoint_id)
    with get_conn() as (conn, cur):
        cur.execute(f"UPDATE appointment SET {', '.join(sets)} WHERE appoint_id=%s", tuple(args))
        conn.commit()

def appointment_delete(appoint_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM appointment WHERE appoint_id=%s", (appoint_id,))
        conn.commit()

# OBSERVATIONS 
def observation_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT o.obser_id, o.obs_year, o.obs_month, o.obs_day,
                   o.appoint_id,
                   p.personnumer AS patient_personnumer, pp.full_name AS patient_name,
                   d.personnumer AS doctor_personnumer, dp.full_name AS doctor_name,
                   o.obs_comment
            FROM observation o
            LEFT JOIN appointment a ON a.appoint_id = o.appoint_id
            LEFT JOIN patient p ON p.personnumer = a.patient_personnumer
            LEFT JOIN person pp ON pp.personnumer = p.personnumer
            LEFT JOIN doctor d ON d.personnumer = a.doctor_personnumer
            LEFT JOIN person dp ON dp.personnumer = d.personnumer
            ORDER BY o.obser_id;
        """)
        return cur.fetchall()

def observation_search(obser_id="", year="", month="", day="", appoint_id="",
                       patient_name="", patient_personnumer="",
                       doctor_name="", doctor_personnumer=""):
    where, args = [], []
    if obser_id:
        where.append("o.obser_id ILIKE %s");       args.append(f"%{obser_id}%")
    if year:
        where.append("o.obs_year = %s");          args.append(_to_int(year))
    if month:
        where.append("o.obs_month = %s");         args.append(_to_int(month))
    if day:
        where.append("o.obs_day = %s");           args.append(_to_int(day))
    if appoint_id:
        where.append("o.appoint_id ILIKE %s");    args.append(f"%{appoint_id}%")
    if patient_name:
        where.append("pp.full_name ILIKE %s");    args.append(f"%{patient_name}%")
    if patient_personnumer:
        where.append("p.personnumer ILIKE %s");   args.append(f"%{patient_personnumer}%")
    if doctor_name:
        where.append("dp.full_name ILIKE %s");    args.append(f"%{doctor_name}%")
    if doctor_personnumer:
        where.append("d.personnumer ILIKE %s");   args.append(f"%{doctor_personnumer}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT o.obser_id, o.obs_year, o.obs_month, o.obs_day,
                   o.appoint_id,
                   p.personnumer AS patient_personnumer, pp.full_name AS patient_name,
                   d.personnumer AS doctor_personnumer, dp.full_name AS doctor_name,
                   o.obs_comment
            FROM observation o
            LEFT JOIN appointment a ON a.appoint_id = o.appoint_id
            LEFT JOIN patient p ON p.personnumer = a.patient_personnumer
            LEFT JOIN person pp ON pp.personnumer = p.personnumer
            LEFT JOIN doctor d ON d.personnumer = a.doctor_personnumer
            LEFT JOIN person dp ON dp.personnumer = d.personnumer
            WHERE {clause}
            ORDER BY o.obser_id;
        """, tuple(args))
        return cur.fetchall()

def observation_insert(obser_id: str, year: str, month: str, day: str,
                       appoint_id: str | None, comment_bytes: bytes | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO observation (obser_id, obs_year, obs_month, obs_day, appoint_id, obs_comment)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (obser_id, _to_int(year), _to_int(month), _to_int(day), (appoint_id or None), comment_bytes))
        conn.commit()

def observation_update(obser_id: str, year: str | None = None, month: str | None = None, day: str | None = None,
                       appoint_id: str | None = None, comment_bytes: bytes | None = None):
    sets, args = [], []
    if not _is_blank(year):    sets.append("obs_year=%s");  args.append(_to_int(year))
    if not _is_blank(month):   sets.append("obs_month=%s"); args.append(_to_int(month))
    if not _is_blank(day):     sets.append("obs_day=%s");   args.append(_to_int(day))
    if not _is_blank(appoint_id): sets.append("appoint_id=%s"); args.append(appoint_id)
    if comment_bytes is not None:
        sets.append("obs_comment=%s"); args.append(comment_bytes)
    if not sets:
        return
    args.append(obser_id)
    with get_conn() as (conn, cur):
        cur.execute(f"UPDATE observation SET {', '.join(sets)} WHERE obser_id=%s", tuple(args))
        conn.commit()

def observation_update_comment(obser_id: str, data: bytes):
    with get_conn() as (conn, cur):
        cur.execute("UPDATE observation SET obs_comment=%s WHERE obser_id=%s", (data, obser_id))
        conn.commit()

def observation_delete(obser_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM observation WHERE obser_id=%s", (obser_id,))
        conn.commit()

# DIAGNOSES 
def diagnosis_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT dg.diagn_id, dg.diagn_year, dg.diagn_month, dg.diagn_day,
                   dg.obser_id, o.appoint_id,
                   p.personnumer AS patient_personnumer, pp.full_name AS patient_name,
                   d.personnumer AS doctor_personnumer, dp.full_name AS doctor_name,
                   dg.diagn_comment
            FROM diagnosis dg
            LEFT JOIN observation o ON o.obser_id = dg.obser_id
            LEFT JOIN appointment a ON a.appoint_id = o.appoint_id
            LEFT JOIN patient p ON p.personnumer = a.patient_personnumer
            LEFT JOIN person pp ON pp.personnumer = p.personnumer
            LEFT JOIN doctor d ON d.personnumer = a.doctor_personnumer
            LEFT JOIN person dp ON dp.personnumer = d.personnumer
            ORDER BY dg.diagn_id;
        """)
        return cur.fetchall()

def diagnosis_search(diagn_id="", year="", month="", day="", obser_id="", appoint_id="",
                     patient_name="", patient_personnumer="",
                     doctor_name="", doctor_personnumer=""):
    where, args = [], []
    if diagn_id:
        where.append("dg.diagn_id ILIKE %s");     args.append(f"%{diagn_id}%")
    if year:
        where.append("dg.diagn_year = %s");       args.append(_to_int(year))
    if month:
        where.append("dg.diagn_month = %s");      args.append(_to_int(month))
    if day:
        where.append("dg.diagn_day = %s");        args.append(_to_int(day))
    if obser_id:
        where.append("dg.obser_id ILIKE %s");     args.append(f"%{obser_id}%")
    if appoint_id:
        where.append("o.appoint_id ILIKE %s");    args.append(f"%{appoint_id}%")
    if patient_name:
        where.append("pp.full_name ILIKE %s");    args.append(f"%{patient_name}%")
    if patient_personnumer:
        where.append("p.personnumer ILIKE %s");   args.append(f"%{patient_personnumer}%")
    if doctor_name:
        where.append("dp.full_name ILIKE %s");    args.append(f"%{doctor_name}%")
    if doctor_personnumer:
        where.append("d.personnumer ILIKE %s");   args.append(f"%{doctor_personnumer}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT dg.diagn_id, dg.diagn_year, dg.diagn_month, dg.diagn_day,
                   dg.obser_id, o.appoint_id,
                   p.personnumer AS patient_personnumer, pp.full_name AS patient_name,
                   d.personnumer AS doctor_personnumer, dp.full_name AS doctor_name,
                   dg.diagn_comment
            FROM diagnosis dg
            LEFT JOIN observation o ON o.obser_id = dg.obser_id
            LEFT JOIN appointment a ON a.appoint_id = o.appoint_id
            LEFT JOIN patient p ON p.personnumer = a.patient_personnumer
            LEFT JOIN person pp ON pp.personnumer = p.personnumer
            LEFT JOIN doctor d ON d.personnumer = a.doctor_personnumer
            LEFT JOIN person dp ON dp.personnumer = d.personnumer
            WHERE {clause}
            ORDER BY dg.diagn_id;
        """, tuple(args))
        return cur.fetchall()

def diagnosis_insert(diagn_id: str, year: str, month: str, day: str,
                     obser_id: str | None, comment_bytes: bytes | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO diagnosis (diagn_id, diagn_year, diagn_month, diagn_day, obser_id, diagn_comment)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (diagn_id, _to_int(year), _to_int(month), _to_int(day), (obser_id or None), comment_bytes))
        conn.commit()

def diagnosis_update(diagn_id: str, year: str | None = None, month: str | None = None, day: str | None = None,
                     obser_id: str | None = None, comment_bytes: bytes | None = None):
    sets, args = [], []
    if not _is_blank(year):    sets.append("diagn_year=%s");  args.append(_to_int(year))
    if not _is_blank(month):   sets.append("diagn_month=%s"); args.append(_to_int(month))
    if not _is_blank(day):     sets.append("diagn_day=%s");   args.append(_to_int(day))
    if not _is_blank(obser_id):sets.append("obser_id=%s");    args.append(obser_id)
    if comment_bytes is not None:
        sets.append("diagn_comment=%s"); args.append(comment_bytes)
    if not sets:
        return
    args.append(diagn_id)
    with get_conn() as (conn, cur):
        cur.execute(f"UPDATE diagnosis SET {', '.join(sets)} WHERE diagn_id=%s", tuple(args))
        conn.commit()

def diagnosis_delete(diagn_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM diagnosis WHERE diagn_id=%s", (diagn_id,))
        conn.commit()

# CLINICS
def clinic_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT c.cli_id, c.cli_name, c.address
            FROM clinic c
            ORDER BY c.cli_id;
        """)
        return cur.fetchall()

def clinic_search(cli_id="", cli_name="", address=""):
    where, args = [], []
    if cli_id:
        where.append("c.cli_id ILIKE %s");   args.append(f"%{cli_id}%")
    if cli_name:
        where.append("c.cli_name ILIKE %s"); args.append(f"%{cli_name}%")
    if address:
        where.append("c.address ILIKE %s");  args.append(f"%{address}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT c.cli_id, c.cli_name, c.address
            FROM clinic c
            WHERE {clause}
            ORDER BY c.cli_id;
        """, tuple(args))
        return cur.fetchall()

def clinic_insert(cli_id: str, cli_name: str, address: str | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO clinic (cli_id, cli_name, address)
            VALUES (%s, %s, %s)
        """, (cli_id, cli_name, (address or None)))
        conn.commit()

def clinic_update(cli_id: str, cli_name: str | None = None, address: str | None = None):
    sets, args = [], []
    if not _is_blank(cli_name): sets.append("cli_name=%s"); args.append(cli_name)
    if not _is_blank(address):  sets.append("address=%s");  args.append(address)
    if not sets:
        return
    args.append(cli_id)
    with get_conn() as (conn, cur):
        cur.execute(f"UPDATE clinic SET {', '.join(sets)} WHERE cli_id=%s", tuple(args))
        conn.commit()

def clinic_delete(cli_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM clinic WHERE cli_id=%s", (cli_id,))
        conn.commit()

# DEPARTMENTS 
def department_view():
    with get_conn() as (conn, cur):
        cur.execute("""
            SELECT d.dept_id, d.dept_name, d.cli_id, c.cli_name
            FROM department d
            LEFT JOIN clinic c ON c.cli_id = d.cli_id
            ORDER BY d.dept_id;
        """)
        return cur.fetchall()

def department_search(dept_id="", dept_name="", cli_id="", clinic_name=""):
    where, args = [], []
    if dept_id:
        where.append("d.dept_id ILIKE %s");   args.append(f"%{dept_id}%")
    if dept_name:
        where.append("d.dept_name ILIKE %s"); args.append(f"%{dept_name}%")
    if cli_id:
        where.append("d.cli_id ILIKE %s");    args.append(f"%{cli_id}%")
    if clinic_name:
        where.append("c.cli_name ILIKE %s");  args.append(f"%{clinic_name}%")
    clause = " AND ".join(where) if where else "1=1"
    with get_conn() as (conn, cur):
        cur.execute(f"""
            SELECT d.dept_id, d.dept_name, d.cli_id, c.cli_name
            FROM department d
            LEFT JOIN clinic c ON c.cli_id = d.cli_id
            WHERE {clause}
            ORDER BY d.dept_id;
        """, tuple(args))
        return cur.fetchall()

def department_insert(dept_id: str, dept_name: str, cli_id: str | None):
    with get_conn() as (conn, cur):
        cur.execute("""
            INSERT INTO department (dept_id, dept_name, cli_id)
            VALUES (%s, %s, %s)
        """, (dept_id, dept_name, (cli_id or None)))
        conn.commit()

def department_update(dept_id: str, dept_name: str | None = None, cli_id: str | None = None):
    sets, args = [], []
    if not _is_blank(dept_name): sets.append("dept_name=%s"); args.append(dept_name)
    if not _is_blank(cli_id):    sets.append("cli_id=%s");    args.append(cli_id)
    if not sets:
        return
    args.append(dept_id)
    with get_conn() as (conn, cur):
        cur.execute(f"UPDATE department SET {', '.join(sets)} WHERE dept_id=%s", tuple(args))
        conn.commit()

def department_delete(dept_id: str):
    with get_conn() as (conn, cur):
        cur.execute("DELETE FROM department WHERE dept_id=%s", (dept_id,))
        conn.commit()
