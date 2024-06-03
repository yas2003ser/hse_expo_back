from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import sqlite3
from sqlite3 import Connection, IntegrityError
from dateutil import parser

app = FastAPI()

DATABASE = 'member.db'

# Utility function to get a database connection
def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

# Models for request and response
class Member(BaseModel):
    id: int
    full_name: str
    team_name: str
    check_in: Optional[str] = None
    check_out: Optional[str] = None

class MemberCreate(BaseModel):
    full_name: str
    team_name: str

class MemberUpdate(BaseModel):
    full_name: Optional[str] = None
    team_name: Optional[str] = None

# Initialize the database
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS members (
                id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                team_name TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS checkin_checkout (
                id INTEGER PRIMARY KEY,
                member_id INTEGER NOT NULL UNIQUE,
                check_in TIMESTAMP,
                check_out TIMESTAMP,
                FOREIGN KEY (member_id) REFERENCES members (id)
            )
        ''')

init_db()

# Utility function to format datetime
def format_datetime(dt: Optional[str]) -> Optional[str]:
    if dt:
        dt_obj = parser.parse(dt)
        return dt_obj.strftime("%d-%m-%Y %I:%M %p")
    return None

@app.post("/members/", response_model=Member)
def create_member(member: MemberCreate, db: Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO members (full_name, team_name) VALUES (?, ?)
    ''', (member.full_name, member.team_name))
    db.commit()
    member_id = cursor.lastrowid
    return Member(
        id=member_id,
        full_name=member.full_name,
        team_name=member.team_name,
        check_in=None,
        check_out=None
    )

@app.get("/members/", response_model=List[Member])
def get_all_members(db: Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM members')
    members = cursor.fetchall()
    results = []
    for member in members:
        cursor.execute('SELECT * FROM checkin_checkout WHERE member_id = ?', (member["id"],))
        checkin_checkout = cursor.fetchone()
        results.append(Member(
            id=member["id"],
            full_name=member["full_name"],
            team_name=member["team_name"],
            check_in=format_datetime(checkin_checkout["check_in"]) if checkin_checkout else None,
            check_out=format_datetime(checkin_checkout["check_out"]) if checkin_checkout else None
        ))
    return results

@app.get("/members/{member_id}", response_model=Member)
def get_member(member_id: int, db: Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM members WHERE id = ?', (member_id,))
    member = cursor.fetchone()
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found")
    
    cursor.execute('SELECT * FROM checkin_checkout WHERE member_id = ?', (member_id,))
    checkin_checkout = cursor.fetchone()
    
    return Member(
        id=member["id"],
        full_name=member["full_name"],
        team_name=member["team_name"],
        check_in=format_datetime(checkin_checkout["check_in"]) if checkin_checkout else None,
        check_out=format_datetime(checkin_checkout["check_out"]) if checkin_checkout else None
    )

@app.put("/members/{member_id}", response_model=Member)
def update_member(member_id: int, member_update: MemberUpdate, db: Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM members WHERE id = ?', (member_id,))
    member = cursor.fetchone()
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found")
    
    update_data = member_update.dict(exclude_unset=True)
    if update_data:
        cursor.execute('''
            UPDATE members SET full_name = COALESCE(?, full_name), team_name = COALESCE(?, team_name) WHERE id = ?
        ''', (update_data.get('full_name'), update_data.get('team_name'), member_id))
        db.commit()
    
    cursor.execute('SELECT * FROM members WHERE id = ?', (member_id,))
    updated_member = cursor.fetchone()
    
    cursor.execute('SELECT * FROM checkin_checkout WHERE member_id = ?', (member_id,))
    checkin_checkout = cursor.fetchone()
    
    return Member(
        id=updated_member["id"],
        full_name=updated_member["full_name"],
        team_name=updated_member["team_name"],
        check_in=format_datetime(checkin_checkout["check_in"]) if checkin_checkout else None,
        check_out=format_datetime(checkin_checkout["check_out"]) if checkin_checkout else None
    )

@app.post("/checkin/{member_id}")
def checkin(member_id: int, db: Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM members WHERE id = ?', (member_id,))
    member = cursor.fetchone()
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found")
    
    cursor.execute('SELECT * FROM checkin_checkout WHERE member_id = ?', (member_id,))
    checkin_checkout = cursor.fetchone()
    
    now = datetime.now()
    if checkin_checkout and checkin_checkout["check_in"]:
        return {"message": f"The member {member['full_name']} is already checked in."}
    
    try:
        cursor.execute('''
            INSERT INTO checkin_checkout (member_id, check_in) VALUES (?, ?)
            ON CONFLICT(member_id) DO UPDATE SET check_in = excluded.check_in
        ''', (member_id, now))
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database integrity error during check-in: {e}")
    
    return {"message": f"The member {member['full_name']} has checked in at {now.strftime('%d-%m-%Y %I:%M %p')}."}

@app.post("/checkout/{member_id}")
def checkout(member_id: int, db: Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute('SELECT * FROM members WHERE id = ?', (member_id,))
    member = cursor.fetchone()
    if member is None:
        raise HTTPException(status_code=404, detail="Member not found")
    
    cursor.execute('SELECT * FROM checkin_checkout WHERE member_id = ?', (member_id,))
    checkin_checkout = cursor.fetchone()
    
    now = datetime.now()
    if checkin_checkout and checkin_checkout["check_out"]:
        return {"message": f"The member {member['full_name']} is already checked out."}
    
    try:
        cursor.execute('''
            INSERT INTO checkin_checkout (member_id, check_out) VALUES (?, ?)
            ON CONFLICT(member_id) DO UPDATE SET check_out = excluded.check_out
        ''', (member_id, now))
        db.commit()
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database integrity error during check-out: {e}")
    
    return {"message": f"The member {member['full_name']} has checked out at {now.strftime('%d-%m-%Y %I:%M %p')}."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
