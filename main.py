from fastapi import FastAPI, Request, Form, Depends, Cookie, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, create_engine, select
from typing import Optional, List
from datetime import datetime
from passlib.context import CryptContext
import secrets, json, os
import uvicorn

class IndustryProfile(SQLModel, table=True):
    __tablename__ = "industry_profiles"
    __table_args__ = {'extend_existing': True} 
    id: Optional[int] = Field(default=None, primary_key=True)
    ocmms_id: Optional[str] = None
    industry_name: str
    industry_type: str
    category: Optional[str] = None
    industry_type_other: Optional[str] = None
    scale_of_unit: Optional[str] = None
    udyam_registered: Optional[str] = None
    udyam_registration_number: Optional[str] = None
    address: str
    district: str
    state: str
    contact_person: str
    designation: Optional[str] = None
    contact_number: str
    email: str

class ResourceAvailability(SQLModel, table=True):
    __tablename__ = "resource_availability"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    total_units: Optional[int] = None
    total_caaqms_stations: Optional[int] = None
    total_stacks_chimneys: Optional[int] = None
    total_etp_plants: Optional[int] = None
    tech_staff_env_dept: int
    is_cems_installed: Optional[str] = None
    dept_looks_after_om: Optional[str] = None
    inhouse_staff_number: int
    inhouse_responsibilities: str
    inhouse_training_attended: str
    vendor_staff_deployed: str 
    vendor_name: Optional[str] = None
    vendor_responsibilities: Optional[str] = None
    vendor_training_attended: Optional[str] = None 
    vendor_training_brief: Optional[str] = None
    vendor_visit_frequency: Optional[str] = None

class ExpensesMonitoring(SQLModel, table=True):
    __tablename__ = "expenses_monitoring"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    purchase_cems: Optional[float] = None
    purchase_ceqms: Optional[float] = None
    purchase_caaqms: Optional[float] = None
    purchase_total: Optional[float] = None
    om_cems: Optional[float] = None
    om_ceqms: Optional[float] = None
    om_caaqms: Optional[float] = None
    om_total: Optional[float] = None
    data_cems: Optional[float] = None
    data_ceqms: Optional[float] = None
    data_caaqms: Optional[float] = None
    data_total: Optional[float] = None
    lab_total: Optional[float] = None

class UnitDetail(SQLModel, table=True):
    __tablename__ = "unit_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    unit_number: str
    name_operation: Optional[str] = None
    capacity_size: Optional[str] = None
    year_commissioning: Optional[int] = None
    is_combustion: Optional[str] = None
    fuel: Optional[str] = None
    attached_to_stack: Optional[str] = None
    apcd_type: Optional[str] = None
    operational_status: Optional[str] = None
    attached_to_etp: Optional[str] = None

class StackDetail(SQLModel, table=True):
    __tablename__ = "stack_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    stack_name: str
    connected_units: Optional[str] = None
    stack_height: Optional[float] = None
    temp: Optional[float] = None
    humidity: Optional[float] = None
    is_cems_connected: Optional[str] = None

class StackParameter(SQLModel, table=True):
    __tablename__ = "stack_parameters"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    stack_id: int = Field(foreign_key="stack_details.id")
    parameter_name: str
    raw_conc: Optional[str] = None
    final_conc: Optional[str] = None

class CaaqmsStationDetail(SQLModel, table=True):
    __tablename__ = "caaqms_station_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    station_name: str
    location: Optional[str] = None
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    parameters_monitored: Optional[str] = None
    monitoring_frequency: Optional[str] = None
    monitoring_frequency_other: Optional[str] = None
    data_connectivity: Optional[str] = None
    data_connectivity_other: Optional[str] = None
    compliance_norms: Optional[str] = None
    operational_status: Optional[str] = None

class EtpDetail(SQLModel, table=True):
    __tablename__ = "etp_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    etp_name: str
    treatment_stages: Optional[str] = None
    design_capacity: Optional[str] = None
    treatment_units: Optional[str] = None
    zld_status: Optional[str] = None
    disposal_pathway: Optional[str] = None
    report_date: Optional[str] = None

class EtpParameter(SQLModel, table=True):
    __tablename__ = "etp_parameters"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    etp_id: int = Field(foreign_key="etp_details.id")
    param_type: str
    param_name: str
    param_value: Optional[str] = None

class CemsStackParam(SQLModel, table=True):
    __tablename__ = "cems_stack_params"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    stack_unit_label: str 
    cems_parameter: str
    emission_level: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    device_id: Optional[str] = None
    technology: Optional[str] = None
    certification: Optional[str] = None
    sampling_method: Optional[str] = None
    install_year: Optional[int] = None
    location: Optional[str] = None
    install_height: Optional[str] = None
    stratification: Optional[str] = None
    regulatory_norms: Optional[str] = None
    range_max: Optional[str] = None
    range_min: Optional[str] = None
    capture_percent: Optional[str] = None

class CemsStackDrift(SQLModel, table=True):
    __tablename__ = "cems_stack_drifts"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    stack_unit_label: str
    parameter: str
    zero_who: Optional[str] = None
    zero_freq: Optional[str] = None
    zero_how: Optional[str] = None
    zero_time: Optional[str] = None
    span_who: Optional[str] = None
    span_freq: Optional[str] = None
    span_how: Optional[str] = None
    span_time: Optional[str] = None
    span_process: Optional[str] = None

class CemsStackCalibration(SQLModel, table=True):
    __tablename__ = "cems_stack_calibrations"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    stack_unit_label: str
    parameter: str
    who_performs: Optional[str] = None
    frequency: Optional[str] = None
    test_carried_out: Optional[str] = None
    points_samples: Optional[str] = None
    operating_loads: Optional[str] = None
    last_calibration_date: Optional[str] = None
    drift_deviation: Optional[str] = None

class CemsStackSummary(SQLModel, table=True):
    __tablename__ = "cems_stack_summary"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    stack_unit_label: str
    comparison_freq: Optional[str] = None
    last_comparison_date: Optional[str] = None
    deviation_noticed: Optional[str] = None

class CemsStackMaintenance(SQLModel, table=True):
    __tablename__ = "cems_stack_maintenances"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    stack_unit_label: str
    parameter: str
    practice: Optional[str] = None

class CemsImplementationLevel(SQLModel, table=True):
    __tablename__ = "cems_implementation_levels"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    query_text: str
    confidence: Optional[str] = None
    managed_by: Optional[str] = None
    explanation: Optional[str] = None
    independent_guidance: Optional[str] = None
    training_preference: Optional[str] = None


class CaaqmsInstallation(SQLModel, table=True):
    __tablename__ = "caaqms_installations"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    caaqms_system_id: str
    monitor: str
    brand: Optional[str] = None
    technology: Optional[str] = None
    vendor: Optional[str] = None
    certification: Optional[str] = None
    install_year: Optional[int] = None
    location: Optional[str] = None
    physical_maintenance: Optional[str] = None
    who_carries: Optional[str] = None
    frequency: Optional[str] = None

class CaaqmsDriftGeneral(SQLModel, table=True):
    __tablename__ = "caaqms_drift_general"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    who_carries_drift: Optional[str] = None
    available_cylinders: Optional[str] = None
    cylinders_connected: Optional[str] = None

class CaaqmsCalibrationCylinder(SQLModel, table=True):
    __tablename__ = "caaqms_calibration_cylinders"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    supplier: Optional[str] = None
    concentration: Optional[str] = None
    expiry_date: Optional[str] = None

class CaaqmsDriftDetail(SQLModel, table=True):
    __tablename__ = "caaqms_drift_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    zero_drift_freq: Optional[str] = None
    zero_drift_process: Optional[str] = None
    span_drift_freq: Optional[str] = None
    span_drift_process: Optional[str] = None
    calib_freq: Optional[str] = None
    calib_process: Optional[str] = None

class CaaqmsMaintenance(SQLModel, table=True):
    __tablename__ = "caaqms_maintenance"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    technology: str
    activities: Optional[str] = None
    frequency: Optional[str] = None
    who_carries: Optional[str] = None
    spare_stock: Optional[str] = None
    contract: Optional[str] = None
    challenges: Optional[str] = None

class CaaqmsImplementationLevel(SQLModel, table=True):
    __tablename__ = "caaqms_implementation_levels"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    query_text: str
    confidence: Optional[str] = None
    managed_by: Optional[str] = None
    explanation: Optional[str] = None
    independent_guidance: Optional[str] = None
    training_preference: Optional[str] = None

class ImplementationChallenge(SQLModel, table=True):
    __tablename__ = "implementation_challenges"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    phase: str
    system_type: str
    challenges: Optional[str] = None
    helped_most: Optional[str] = None
    how_it_helped: Optional[str] = None
    regulatory_support: Optional[str] = None

class ExpectedImprovement(SQLModel, table=True):
    __tablename__ = "expected_improvements"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    suggestion_1: str
    suggestion_2: Optional[str] = None
    suggestion_3: Optional[str] = None
    suggestion_4: Optional[str] = None
    suggestion_5: Optional[str] = None

class User(SQLModel, table=True):
    __tablename__ = "users"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    email: str = Field(unique=True, index=True)
    hashed_password: str
    industry_name: str
    created_at: Optional[str] = None
    survey_submitted: Optional[bool] = Field(default=False)

class DraftSurvey(SQLModel, table=True):
    __tablename__ = "draft_surveys"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    user_id: int = Field(foreign_key="users.id")
    last_step: int = Field(default=1)
    form_data_json: Optional[str] = None
    updated_at: Optional[str] = None

DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
sqlite_file_name = os.path.join(DATA_DIR, "database.db")
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=False)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

create_db_and_tables()

def get_session():
    with Session(engine) as session:
        yield session

pwd_context = CryptContext(schemes=["pbkdf2_sha256", "bcrypt"], deprecated="auto")
_sessions: dict = {}

def hash_password(plain: str) -> str:
    return pwd_context.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    try:
        return pwd_context.verify(plain, hashed)
    except Exception:
        return pwd_context.verify(plain[:71], hashed)

def create_session_token(user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    _sessions[token] = user_id
    return token

def get_current_user_id(session_token: Optional[str] = Cookie(default=None)) -> Optional[int]:
    if not session_token:
        return None
    return _sessions.get(session_token)

def require_user(session_token: Optional[str] = Cookie(default=None)) -> int:
    user_id = _sessions.get(session_token) if session_token else None
    if not user_id:
        raise HTTPException(status_code=302, headers={"Location": "/login"})
    return user_id

from fastapi.staticfiles import StaticFiles
app = FastAPI()
if os.path.isdir("assets"):
    app.mount("/assets", StaticFiles(directory="assets"), name="assets")
templates = Jinja2Templates(directory="templates")

def render_template(name: str, context: dict):
    try:
        return templates.TemplateResponse(context["request"], name, context)
    except Exception:
        return templates.TemplateResponse(name, context)

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, msg: str = ""):
    return render_template("login.html", {"request": request, "msg": msg})

@app.post("/login")
async def do_login(request: Request, email: str = Form(...), password: str = Form(...), session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.email == email)).first()
    if not user or not verify_password(password, user.hashed_password):
        return render_template("login.html", {"request": request, "msg": "Invalid email or password."})
    token = create_session_token(user.id)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(key="session_token", value=token, httponly=True)
    return response

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, msg: str = ""):
    return render_template("register.html", {"request": request, "msg": msg})

@app.post("/register")
async def do_register(request: Request, industry_name: str = Form(...), email: str = Form(...), password: str = Form(...), confirm_password: str = Form(...), session: Session = Depends(get_session)):
    if password != confirm_password:
        return render_template("register.html", {"request": request, "msg": "Passwords do not match."})
    existing = session.exec(select(User).where(User.email == email)).first()
    if existing:
        return render_template("register.html", {"request": request, "msg": "An account with this email already exists."})
    new_user = User(email=email, hashed_password=hash_password(password), industry_name=industry_name, created_at=datetime.now().isoformat())
    session.add(new_user)
    session.commit()
    session.refresh(new_user)
    token = create_session_token(new_user.id)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(key="session_token", value=token, httponly=True)
    return response

@app.get("/logout")
async def logout(session_token: Optional[str] = Cookie(default=None)):
    if session_token and session_token in _sessions:
        del _sessions[session_token]
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session_token")
    return response

@app.get("/forgot-password", response_class=HTMLResponse)
async def forgot_password_page(request: Request, msg: str = "", email_found: str = ""):
    return render_template("forgot_password.html", {"request": request, "msg": msg, "email_found": email_found})

@app.post("/forgot-password")
async def do_forgot_password(request: Request, action: str = Form(...), email: str = Form(...), new_password: str = Form(""), confirm_new: str = Form(""), session: Session = Depends(get_session)):
    user = session.exec(select(User).where(User.email == email)).first()
    if action == "verify":
        if not user:
            return render_template("forgot_password.html", {"request": request, "msg": "No account found with that email.", "email_found": ""})
        return render_template("forgot_password.html", {"request": request, "msg": "", "email_found": email})
    elif action == "reset":
        if not user:
            return render_template("forgot_password.html", {"request": request, "msg": "Error. Please try again.", "email_found": ""})
        if new_password != confirm_new:
            return render_template("forgot_password.html", {"request": request, "msg": "Passwords do not match.", "email_found": email})
        if len(new_password) < 6:
            return render_template("forgot_password.html", {"request": request, "msg": "Password must be at least 6 characters.", "email_found": email})
        user.hashed_password = hash_password(new_password)
        session.add(user)
        session.commit()
        return RedirectResponse(url="/login?msg=Password+updated+successfully", status_code=302)

@app.post("/save_draft")
async def save_draft(request: Request, session: Session = Depends(get_session), session_token: Optional[str] = Cookie(default=None)):
    user_id = _sessions.get(session_token) if session_token else None
    if not user_id:
        return {"status": "error", "msg": "Not logged in"}
    body = await request.json()
    form_data = body.get("form_data", {})
    last_step = body.get("last_step", 1)
    draft = session.exec(select(DraftSurvey).where(DraftSurvey.user_id == user_id)).first()
    if draft:
        draft.form_data_json = json.dumps(form_data)
        draft.last_step = last_step
        draft.updated_at = datetime.now().isoformat()
        session.add(draft)
    else:
        draft = DraftSurvey(user_id=user_id, last_step=last_step, form_data_json=json.dumps(form_data), updated_at=datetime.now().isoformat())
        session.add(draft)
    session.commit()
    return {"status": "ok"}

@app.get("/get_draft")
async def get_draft(session: Session = Depends(get_session), session_token: Optional[str] = Cookie(default=None)):
    user_id = _sessions.get(session_token) if session_token else None
    if not user_id:
        return {"status": "none"}
    draft = session.exec(select(DraftSurvey).where(DraftSurvey.user_id == user_id)).first()
    if not draft or not draft.form_data_json:
        return {"status": "none"}
    return {"status": "ok", "last_step": draft.last_step, "form_data": json.loads(draft.form_data_json)}

@app.get("/", response_class=HTMLResponse)
async def show_form(request: Request, session_token: Optional[str] = Cookie(default=None), session: Session = Depends(get_session)):
    user_id = _sessions.get(session_token) if session_token else None
    if not user_id:
        return RedirectResponse(url="/login", status_code=302)
    user = session.get(User, user_id)
    industry_name_prefill = user.industry_name if user else ""
    industry_types = [
        "Aluminum Smelter", "Biomass-based Power Plant", "Biomedical Waste Incinerator", "CETP",
        "Caustic Soda", "Cement", "Chemicals", "Common Hazardous Waste Incinerator", "Copper Smelter",
        "Distilleries", "Dyes & Dye Intermediates", "Fertilizer", "Food, Dairy and Beverage",
        "Iron and Steel", "Leather / Tannery", "Oil Refineries", "Pesticides", "Petrochemicals",
        "Pharmaceuticals", "Pulp and Paper", "STP", "Slaughter House", "Sugar", "Textile",
        "Thermal Power Plants", "Zinc Smelter", "Others - Metal Processing", "Others - Paint",
        "Others - Wood and Furniture manufacturing", "Other"
    ]
    states = ["Andaman and Nicobar Islands", "Andhra Pradesh", "Assam", "Bihar", "Chhattisgarh", "Delhi", "Goa", "Gujarat", "Haryana", "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Odisha", "Punjab", "Rajasthan", "Tamil Nadu", "Telangana", "Uttar Pradesh", "West Bengal"]
    return render_template("index.html", {"request": request, "industry_types": industry_types, "states": states, "industry_name_prefill": industry_name_prefill, "user_email": user.email if user else ""})

@app.post("/submit_all")
async def submit_all(
    request: Request,
    session_token: Optional[str] = Cookie(default=None),
    ocmms_id: str = Form(...), industry_name: str = Form(...), industry_type: str = Form(...), category: str = Form(...), industry_type_other: Optional[str] = Form(None), scale_of_unit: str = Form(...), udyam_registered: Optional[str] = Form(None), udyam_registration_number: Optional[str] = Form(None), address: str = Form(...), district: str = Form(...), state: str = Form(...), contact_person: str = Form(...), designation: Optional[str] = Form(None), contact_number: str = Form(...), email: str = Form(...),
    total_units: str = Form(...), total_caaqms_stations: str = Form(...), total_stacks_chimneys: str = Form(...), total_etp_plants: str = Form(...), tech_staff_env_dept: str = Form(...), is_cems_installed: str = Form(...), dept_looks_after_om: Optional[str] = Form(None), inhouse_staff_number: str = Form(...), inhouse_responsibilities: str = Form(...), inhouse_training_attended: str = Form(...), vendor_staff_deployed: str = Form(...), vendor_name: Optional[str] = Form(None), vendor_responsibilities: Optional[str] = Form(None), vendor_training_attended: Optional[str] = Form(None), vendor_training_brief: Optional[str] = Form(None), vendor_visit_frequency: Optional[str] = Form(None),
    purchase_cems: Optional[str] = Form(None), purchase_ceqms: Optional[str] = Form(None), purchase_caaqms: Optional[str] = Form(None), purchase_total: Optional[str] = Form(None), om_cems: Optional[str] = Form(None), om_ceqms: Optional[str] = Form(None), om_caaqms: Optional[str] = Form(None), om_total: Optional[str] = Form(None), data_cems: Optional[str] = Form(None), data_ceqms: Optional[str] = Form(None), data_caaqms: Optional[str] = Form(None), data_total: Optional[str] = Form(None), lab_total: Optional[str] = Form(None),
    unit_number: List[str] = Form(None), name_operation: List[str] = Form(None), capacity_size: List[str] = Form(None), year_commissioning: List[str] = Form(None), is_combustion: List[str] = Form(None), fuel: List[str] = Form(None), attached_to_stack: List[str] = Form(None), apcd_type: List[str] = Form(None), operational_status: List[str] = Form(None), attached_to_etp: List[str] = Form(None),
    stack_name: List[str] = Form(None), connected_units: List[str] = Form(None), stack_height: List[str] = Form(None), stack_temp: List[str] = Form(None), stack_humidity: List[str] = Form(None), stack_is_cems_connected: List[str] = Form(None),
    stack_param_parent: List[str] = Form(None), stack_param_name: List[str] = Form(None), stack_param_raw: List[str] = Form(None), stack_param_final: List[str] = Form(None),
    
    cems_stack_label: List[str] = Form(None), cems_stack_param: List[str] = Form(None), cems_stack_emission: List[str] = Form(None), cems_stack_make: List[str] = Form(None), cems_stack_model: List[str] = Form(None), cems_stack_device: List[str] = Form(None), cems_stack_tech: List[str] = Form(None), cems_stack_cert: List[str] = Form(None), cems_stack_sampling: List[str] = Form(None), cems_stack_year: List[str] = Form(None), cems_stack_loc: List[str] = Form(None), cems_stack_height: List[str] = Form(None), cems_stack_strat: List[str] = Form(None), cems_stack_norms: List[str] = Form(None), cems_stack_max: List[str] = Form(None), cems_stack_min: List[str] = Form(None), cems_stack_capture_percent: List[str] = Form(None),
    cems_drift_stack_label: List[str] = Form(None), cems_drift_param: List[str] = Form(None), cems_drift_zero_who: List[str] = Form(None), cems_drift_zero_freq: List[str] = Form(None), cems_drift_zero_how: List[str] = Form(None), cems_drift_zero_time: List[str] = Form(None), cems_drift_span_who: List[str] = Form(None), cems_drift_span_freq: List[str] = Form(None), cems_drift_span_how: List[str] = Form(None), cems_drift_span_time: List[str] = Form(None), cems_drift_span_process: List[str] = Form(None),
    cems_calib_stack_label: List[str] = Form(None), cems_calib_param: List[str] = Form(None), cems_calib_who: List[str] = Form(None), cems_calib_freq: List[str] = Form(None), cems_calib_test: List[str] = Form(None), cems_calib_points: List[str] = Form(None), cems_calib_loads: List[str] = Form(None), cems_calib_last_date: List[str] = Form(None), cems_calib_drift: List[str] = Form(None),
    cems_summary_stack_label: List[str] = Form(None), cems_summary_comparison_freq: List[str] = Form(None), cems_summary_last_date: List[str] = Form(None), cems_summary_deviation: List[str] = Form(None),
    cems_maint_stack_label: List[str] = Form(None), cems_maint_param: List[str] = Form(None), cems_maint_practice: List[str] = Form(None),
    impl_query: List[str] = Form(None), impl_confidence: List[str] = Form(None), impl_managed_by: List[str] = Form(None), impl_explanation: List[str] = Form(None), impl_guidance: List[str] = Form(None), impl_training: List[str] = Form(None),

    caaqms_inst_sys_id: List[str] = Form(None), caaqms_inst_monitor: List[str] = Form(None), caaqms_inst_brand: List[str] = Form(None), caaqms_inst_tech: List[str] = Form(None), caaqms_inst_vendor: List[str] = Form(None), caaqms_inst_cert: List[str] = Form(None), caaqms_inst_year: List[str] = Form(None), caaqms_inst_loc: List[str] = Form(None), caaqms_inst_maint: List[str] = Form(None), caaqms_inst_who: List[str] = Form(None), caaqms_inst_freq: List[str] = Form(None),
    caaqms_drift_who_carries: Optional[str] = Form(None), caaqms_drift_available_cylinders: Optional[str] = Form(None), caaqms_drift_cylinders_connected: Optional[str] = Form(None),
    caaqms_cyl_parameter: List[str] = Form(None), caaqms_cyl_supplier: List[str] = Form(None), caaqms_cyl_concentration: List[str] = Form(None), caaqms_cyl_expiry: List[str] = Form(None),
    caaqms_drift_parameter: List[str] = Form(None), caaqms_drift_zero_freq: List[str] = Form(None), caaqms_drift_zero_process: List[str] = Form(None), caaqms_drift_span_freq: List[str] = Form(None), caaqms_drift_span_process: List[str] = Form(None), caaqms_calib_freq: List[str] = Form(None), caaqms_calib_process: List[str] = Form(None),
    caaqms_maint_tech: List[str] = Form(None), caaqms_maint_act: List[str] = Form(None), caaqms_maint_freq: List[str] = Form(None), caaqms_maint_who: List[str] = Form(None), caaqms_maint_stock: List[str] = Form(None), caaqms_maint_contract: List[str] = Form(None), caaqms_maint_challenges: List[str] = Form(None),
    caaqms_impl_query: List[str] = Form(None), caaqms_impl_confidence: List[str] = Form(None), caaqms_impl_managed_by: List[str] = Form(None), caaqms_impl_explanation: List[str] = Form(None), caaqms_impl_guidance: List[str] = Form(None), caaqms_impl_training: List[str] = Form(None),
    challenge_phase: List[str] = Form(None), challenge_system: List[str] = Form(None), challenges: List[str] = Form(None), challenge_helped_most: List[str] = Form(None), challenge_how_helped: List[str] = Form(None), regulatory_support: List[str] = Form(None),
    suggestion_1: str = Form(...), suggestion_2: Optional[str] = Form(None), suggestion_3: Optional[str] = Form(None), suggestion_4: Optional[str] = Form(None), suggestion_5: Optional[str] = Form(None),
    caaqms_station_name: List[str] = Form(None), caaqms_location: List[str] = Form(None), caaqms_latitude: List[str] = Form(None), caaqms_longitude: List[str] = Form(None), caaqms_params: List[str] = Form(None), caaqms_freq: List[str] = Form(None), caaqms_freq_other: List[str] = Form(None), caaqms_connectivity: List[str] = Form(None), caaqms_connectivity_other: List[str] = Form(None), caaqms_compliance: List[str] = Form(None), caaqms_status: List[str] = Form(None),
    etp_name: List[str] = Form(None), etp_stages: List[str] = Form(None), etp_capacity: List[str] = Form(None), etp_units: List[str] = Form(None), etp_zld: List[str] = Form(None), etp_disposal: List[str] = Form(None), etp_report_date: List[str] = Form(None),
    etp_param_parent: List[str] = Form(None), etp_param_type: List[str] = Form(None), etp_param_name: List[str] = Form(None), etp_param_value: List[str] = Form(None),
    session: Session = Depends(get_session)
):
    def int_or_none(lst, idx):
        if not lst or idx >= len(lst): return None
        v = str(lst[idx]).strip().upper()
        if v in ('', 'NA', 'N/A', 'NONE'): return None
        try: return int(v)
        except (ValueError, TypeError): return None

    def float_or_none(v):
        if v is None: return None
        v = str(v).strip().upper()
        if v in ('', 'NA', 'N/A', 'NONE'): return None
        try: return float(v)
        except (ValueError, TypeError): return None

    def safe_int(v):
        if v is None: return 0
        v = str(v).strip().upper()
        if v in ('', 'NA', 'N/A', 'NONE'): return 0
        try: return int(v)
        except (ValueError, TypeError): return 0

    new_profile = IndustryProfile(ocmms_id=ocmms_id, industry_name=industry_name, industry_type=industry_type, category=category, industry_type_other=industry_type_other, scale_of_unit=scale_of_unit, udyam_registered=udyam_registered, udyam_registration_number=udyam_registration_number, address=address, district=district, state=state, contact_person=contact_person, designation=designation, contact_number=contact_number, email=email)
    session.add(new_profile)
    session.commit()
    session.refresh(new_profile) 

    new_resource = ResourceAvailability(industry_id=new_profile.id, total_units=safe_int(total_units), total_caaqms_stations=safe_int(total_caaqms_stations), total_stacks_chimneys=safe_int(total_stacks_chimneys), total_etp_plants=safe_int(total_etp_plants), tech_staff_env_dept=safe_int(tech_staff_env_dept), is_cems_installed=is_cems_installed, dept_looks_after_om=dept_looks_after_om, inhouse_staff_number=safe_int(inhouse_staff_number), inhouse_responsibilities=inhouse_responsibilities, inhouse_training_attended=inhouse_training_attended, vendor_staff_deployed=vendor_staff_deployed, vendor_name=vendor_name, vendor_responsibilities=vendor_responsibilities, vendor_training_attended=vendor_training_attended, vendor_training_brief=vendor_training_brief, vendor_visit_frequency=vendor_visit_frequency)
    session.add(new_resource)
    
    new_expenses = ExpensesMonitoring(industry_id=new_profile.id, purchase_cems=float_or_none(purchase_cems), purchase_ceqms=float_or_none(purchase_ceqms), purchase_caaqms=float_or_none(purchase_caaqms), purchase_total=float_or_none(purchase_total), om_cems=float_or_none(om_cems), om_ceqms=float_or_none(om_ceqms), om_caaqms=float_or_none(om_caaqms), om_total=float_or_none(om_total), data_cems=float_or_none(data_cems), data_ceqms=float_or_none(data_ceqms), data_caaqms=float_or_none(data_caaqms), data_total=float_or_none(data_total), lab_total=float_or_none(lab_total))
    session.add(new_expenses)
    if unit_number and len(unit_number) > 0:
        for i in range(len(unit_number)):
            unit = UnitDetail(industry_id=new_profile.id, unit_number=unit_number[i], name_operation=name_operation[i] if len(name_operation)>i else None, capacity_size=capacity_size[i] if len(capacity_size)>i else None, year_commissioning=int_or_none(year_commissioning, i), is_combustion=is_combustion[i] if is_combustion and len(is_combustion)>i else None, fuel=fuel[i] if fuel and len(fuel)>i else None, attached_to_stack=attached_to_stack[i] if attached_to_stack and len(attached_to_stack)>i else None, apcd_type=apcd_type[i] if apcd_type and len(apcd_type)>i else None, operational_status=operational_status[i] if operational_status and len(operational_status)>i else None, attached_to_etp=attached_to_etp[i] if attached_to_etp and len(attached_to_etp)>i else None)
            session.add(unit)
        session.flush()

    if stack_name and len(stack_name) > 0:
        for i in range(len(stack_name)):
            stack = StackDetail(
                industry_id=new_profile.id,
                stack_name=stack_name[i],
                connected_units=connected_units[i] if connected_units and len(connected_units)>i else None,
                stack_height=float_or_none(stack_height[i]) if stack_height and len(stack_height)>i else None,
                temp=float_or_none(stack_temp[i]) if stack_temp and len(stack_temp)>i else None,
                humidity=float_or_none(stack_humidity[i]) if stack_humidity and len(stack_humidity)>i else None,
                is_cems_connected=stack_is_cems_connected[i] if stack_is_cems_connected and len(stack_is_cems_connected)>i else None
            )
            session.add(stack)
            session.flush()

            if stack_param_parent:
                for p_idx in range(len(stack_param_parent)):
                    if stack_param_parent[p_idx] == stack_name[i]:
                        param = StackParameter(
                            stack_id=stack.id,
                            parameter_name=stack_param_name[p_idx],
                            raw_conc=stack_param_raw[p_idx],
                            final_conc=stack_param_final[p_idx]
                        )
                        session.add(param)
        session.flush()

    if caaqms_station_name and len(caaqms_station_name) > 0:
        for i in range(len(caaqms_station_name)):
            station = CaaqmsStationDetail(
                industry_id=new_profile.id,
                station_name=caaqms_station_name[i],
                location=caaqms_location[i] if caaqms_location and len(caaqms_location)>i else None,
                latitude=caaqms_latitude[i] if caaqms_latitude and len(caaqms_latitude)>i else None,
                longitude=caaqms_longitude[i] if caaqms_longitude and len(caaqms_longitude)>i else None,
                parameters_monitored=caaqms_params[i] if caaqms_params and len(caaqms_params)>i else None,
                monitoring_frequency=caaqms_freq[i] if caaqms_freq and len(caaqms_freq)>i else None,
                monitoring_frequency_other=caaqms_freq_other[i] if caaqms_freq_other and len(caaqms_freq_other)>i else None,
                data_connectivity=caaqms_connectivity[i] if caaqms_connectivity and len(caaqms_connectivity)>i else None,
                data_connectivity_other=caaqms_connectivity_other[i] if caaqms_connectivity_other and len(caaqms_connectivity_other)>i else None,
                compliance_norms=caaqms_compliance[i] if caaqms_compliance and len(caaqms_compliance)>i else None,
                operational_status=caaqms_status[i] if caaqms_status and len(caaqms_status)>i else None
            )
            session.add(station)
        session.flush()

    if etp_name and len(etp_name) > 0:
        for i in range(len(etp_name)):
            etp = EtpDetail(
                industry_id=new_profile.id,
                etp_name=etp_name[i],
                treatment_stages=etp_stages[i] if etp_stages and len(etp_stages)>i else None,
                design_capacity=etp_capacity[i] if etp_capacity and len(etp_capacity)>i else None,
                treatment_units=etp_units[i] if etp_units and len(etp_units)>i else None,
                zld_status=etp_zld[i] if etp_zld and len(etp_zld)>i else None,
                disposal_pathway=etp_disposal[i] if etp_disposal and len(etp_disposal)>i else None,
                report_date=etp_report_date[i] if etp_report_date and len(etp_report_date)>i else None
            )
            session.add(etp)
            session.flush()

            if etp_param_parent:
                for p_idx in range(len(etp_param_parent)):
                    if etp_param_parent[p_idx] == etp_name[i]:
                        param = EtpParameter(
                            etp_id=etp.id,
                            param_type=etp_param_type[p_idx],
                            param_name=etp_param_name[p_idx],
                            param_value=etp_param_value[p_idx]
                        )
                        session.add(param)
        session.flush()

    if cems_stack_label and len(cems_stack_label) > 0:
        for i in range(len(cems_stack_label)):
            if cems_stack_param[i] and cems_stack_param[i].strip() != "":
                cems_item = CemsStackParam(
                    industry_id=new_profile.id,
                    stack_unit_label=cems_stack_label[i],
                    cems_parameter=cems_stack_param[i],
                    emission_level=cems_stack_emission[i] if len(cems_stack_emission)>i else None,
                    brand=cems_stack_make[i] if len(cems_stack_make)>i else None,
                    model=cems_stack_model[i] if len(cems_stack_model)>i else None,
                    device_id=cems_stack_device[i] if len(cems_stack_device)>i else None,
                    technology=cems_stack_tech[i] if len(cems_stack_tech)>i else None,
                    certification=cems_stack_cert[i] if len(cems_stack_cert)>i else None,
                    sampling_method=cems_stack_sampling[i] if len(cems_stack_sampling)>i else None,
                    install_year=int_or_none(cems_stack_year, i),
                    location=cems_stack_loc[i] if len(cems_stack_loc)>i else None,
                    install_height=cems_stack_height[i] if len(cems_stack_height)>i else None,
                    stratification=cems_stack_strat[i] if len(cems_stack_strat)>i else None,
                    regulatory_norms=cems_stack_norms[i] if len(cems_stack_norms)>i else None,
                    range_max=cems_stack_max[i] if len(cems_stack_max)>i else None,
                    range_min=cems_stack_min[i] if len(cems_stack_min)>i else None,
                    capture_percent=cems_stack_capture_percent[i] if len(cems_stack_capture_percent)>i else None
                )
                session.add(cems_item)
        session.flush()

    if cems_drift_stack_label:
        for i in range(len(cems_drift_stack_label)):
            if cems_drift_param[i] and cems_drift_param[i].strip() != "":
                drift = CemsStackDrift(
                    industry_id=new_profile.id,
                    stack_unit_label=cems_drift_stack_label[i],
                    parameter=cems_drift_param[i],
                    zero_who=cems_drift_zero_who[i] if len(cems_drift_zero_who)>i else None,
                    zero_freq=cems_drift_zero_freq[i] if len(cems_drift_zero_freq)>i else None,
                    zero_how=cems_drift_zero_how[i] if len(cems_drift_zero_how)>i else None,
                    zero_time=cems_drift_zero_time[i] if len(cems_drift_zero_time)>i else None,
                    span_who=cems_drift_span_who[i] if len(cems_drift_span_who)>i else None,
                    span_freq=cems_drift_span_freq[i] if len(cems_drift_span_freq)>i else None,
                    span_how=cems_drift_span_how[i] if len(cems_drift_span_how)>i else None,
                    span_time=cems_drift_span_time[i] if len(cems_drift_span_time)>i else None,
                    span_process=cems_drift_span_process[i] if len(cems_drift_span_process)>i else None
                )
                session.add(drift)
        session.flush()

    if cems_calib_stack_label:
        for i in range(len(cems_calib_stack_label)):
            if cems_calib_param[i] and cems_calib_param[i].strip() != "":
                calib = CemsStackCalibration(
                    industry_id=new_profile.id,
                    stack_unit_label=cems_calib_stack_label[i],
                    parameter=cems_calib_param[i],
                    who_performs=cems_calib_who[i] if len(cems_calib_who)>i else None,
                    frequency=cems_calib_freq[i] if len(cems_calib_freq)>i else None,
                    test_carried_out=cems_calib_test[i] if len(cems_calib_test)>i else None,
                    points_samples=cems_calib_points[i] if len(cems_calib_points)>i else None,
                    operating_loads=cems_calib_loads[i] if len(cems_calib_loads)>i else None,
                    last_calibration_date=cems_calib_last_date[i] if len(cems_calib_last_date)>i else None,
                    drift_deviation=cems_calib_drift[i] if len(cems_calib_drift)>i else None
                )
                session.add(calib)
        session.flush()

    if cems_summary_stack_label:
        for i in range(len(cems_summary_stack_label)):
            summary = CemsStackSummary(
                industry_id=new_profile.id,
                stack_unit_label=cems_summary_stack_label[i],
                comparison_freq=cems_summary_comparison_freq[i] if len(cems_summary_comparison_freq)>i else None,
                last_comparison_date=cems_summary_last_date[i] if len(cems_summary_last_date)>i else None,
                deviation_noticed=cems_summary_deviation[i] if len(cems_summary_deviation)>i else None
            )
            session.add(summary)
        session.flush()

    if cems_maint_stack_label:
        for i in range(len(cems_maint_stack_label)):
            if cems_maint_param[i] and cems_maint_param[i].strip() != "":
                maint_item = CemsStackMaintenance(
                    industry_id=new_profile.id,
                    stack_unit_label=cems_maint_stack_label[i],
                    parameter=cems_maint_param[i],
                    practice=cems_maint_practice[i] if len(cems_maint_practice)>i else None
                )
                session.add(maint_item)
        session.flush()

    if impl_query:
        for i in range(len(impl_query)):
            impl = CemsImplementationLevel(industry_id=new_profile.id, query_text=impl_query[i], confidence=impl_confidence[i] if len(impl_confidence)>i else None, managed_by=impl_managed_by[i] if len(impl_managed_by)>i else None, explanation=impl_explanation[i] if len(impl_explanation)>i else None, independent_guidance=impl_guidance[i] if len(impl_guidance)>i else None, training_preference=impl_training[i] if len(impl_training)>i else None)
            session.add(impl)

    if caaqms_inst_sys_id and len(caaqms_inst_sys_id) > 0:
        for i in range(len(caaqms_inst_sys_id)):
            if caaqms_inst_monitor[i]:
                inst = CaaqmsInstallation(industry_id=new_profile.id, caaqms_system_id=caaqms_inst_sys_id[i], monitor=caaqms_inst_monitor[i], brand=caaqms_inst_brand[i] if len(caaqms_inst_brand)>i else None, technology=caaqms_inst_tech[i] if len(caaqms_inst_tech)>i else None, vendor=caaqms_inst_vendor[i] if len(caaqms_inst_vendor)>i else None, certification=caaqms_inst_cert[i] if len(caaqms_inst_cert)>i else None, install_year=int_or_none(caaqms_inst_year, i), location=caaqms_inst_loc[i] if len(caaqms_inst_loc)>i else None, physical_maintenance=caaqms_inst_maint[i] if len(caaqms_inst_maint)>i else None, who_carries=caaqms_inst_who[i] if len(caaqms_inst_who)>i else None, frequency=caaqms_inst_freq[i] if len(caaqms_inst_freq)>i else None)
                session.add(inst)

    if caaqms_drift_who_carries or caaqms_drift_available_cylinders or caaqms_drift_cylinders_connected:
        caaqms_drift_gen = CaaqmsDriftGeneral(industry_id=new_profile.id, who_carries_drift=caaqms_drift_who_carries, available_cylinders=caaqms_drift_available_cylinders, cylinders_connected=caaqms_drift_cylinders_connected)
        session.add(caaqms_drift_gen)
    
    if caaqms_cyl_parameter:
        for i in range(len(caaqms_cyl_parameter)):
            caaqms_cyl = CaaqmsCalibrationCylinder(industry_id=new_profile.id, parameter=caaqms_cyl_parameter[i], supplier=caaqms_cyl_supplier[i] if len(caaqms_cyl_supplier)>i else None, concentration=caaqms_cyl_concentration[i] if len(caaqms_cyl_concentration)>i else None, expiry_date=caaqms_cyl_expiry[i] if len(caaqms_cyl_expiry)>i else None)
            session.add(caaqms_cyl)

    if caaqms_drift_parameter:
        for i in range(len(caaqms_drift_parameter)):
            caaqms_drift_det = CaaqmsDriftDetail(industry_id=new_profile.id, parameter=caaqms_drift_parameter[i], zero_drift_freq=caaqms_drift_zero_freq[i] if len(caaqms_drift_zero_freq)>i else None, zero_drift_process=caaqms_drift_zero_process[i] if len(caaqms_drift_zero_process)>i else None, span_drift_freq=caaqms_drift_span_freq[i] if len(caaqms_drift_span_freq)>i else None, span_drift_process=caaqms_drift_span_process[i] if len(caaqms_drift_span_process)>i else None, calib_freq=caaqms_calib_freq[i] if len(caaqms_calib_freq)>i else None, calib_process=caaqms_calib_process[i] if len(caaqms_calib_process)>i else None)
            session.add(caaqms_drift_det)

    if caaqms_maint_tech:
        for i in range(len(caaqms_maint_tech)):
            if caaqms_maint_tech[i].strip() != "":
                caaqms_maint = CaaqmsMaintenance(industry_id=new_profile.id, technology=caaqms_maint_tech[i], activities=caaqms_maint_act[i] if len(caaqms_maint_act)>i else None, frequency=caaqms_maint_freq[i] if len(caaqms_maint_freq)>i else None, who_carries=caaqms_maint_who[i] if len(caaqms_maint_who)>i else None, spare_stock=caaqms_maint_stock[i] if len(caaqms_maint_stock)>i else None, contract=caaqms_maint_contract[i] if len(caaqms_maint_contract)>i else None, challenges=caaqms_maint_challenges[i] if len(caaqms_maint_challenges)>i else None)
                session.add(caaqms_maint)

    if caaqms_impl_query:
        for i in range(len(caaqms_impl_query)):
            caaqms_impl = CaaqmsImplementationLevel(industry_id=new_profile.id, query_text=caaqms_impl_query[i], confidence=caaqms_impl_confidence[i] if len(caaqms_impl_confidence)>i else None, managed_by=caaqms_impl_managed_by[i] if len(caaqms_impl_managed_by)>i else None, explanation=caaqms_impl_explanation[i] if len(caaqms_impl_explanation)>i else None, independent_guidance=caaqms_impl_guidance[i] if len(caaqms_impl_guidance)>i else None, training_preference=caaqms_impl_training[i] if len(caaqms_impl_training)>i else None)
            session.add(caaqms_impl)


    if challenge_phase and len(challenge_phase) > 0:
        for i in range(len(challenge_phase)):
            impl_challenge = ImplementationChallenge(industry_id=new_profile.id, phase=challenge_phase[i], system_type=challenge_system[i] if len(challenge_system)>i else None, challenges=challenges[i] if challenges and len(challenges)>i else None, helped_most=challenge_helped_most[i] if len(challenge_helped_most)>i else None, how_it_helped=challenge_how_helped[i] if len(challenge_how_helped)>i else None, regulatory_support=regulatory_support[i] if len(regulatory_support)>i else None)
            session.add(impl_challenge)

    new_improvement = ExpectedImprovement(industry_id=new_profile.id, suggestion_1=suggestion_1, suggestion_2=suggestion_2, suggestion_3=suggestion_3, suggestion_4=suggestion_4, suggestion_5=suggestion_5)
    session.add(new_improvement)

    session.commit()
    user_id = _sessions.get(session_token) if session_token else None
    if user_id:
        draft = session.exec(select(DraftSurvey).where(DraftSurvey.user_id == user_id)).first()
        if draft:
            session.delete(draft)
        user_obj = session.get(User, user_id)
        if user_obj:
            user_obj.survey_submitted = True
            session.add(user_obj)
        session.commit()
    submitted_date = datetime.now().strftime("%d %b %Y, %I:%M %p")
    return render_template("success.html", {
        "request": request,
        "submission_id": f"CEEW-{new_profile.id:04d}",
        "industry_name": industry_name,
        "submitted_date": submitted_date,
    })

@app.get("/download-db")
def download_db(token: str = ""):
    expected = os.environ.get("DB_DOWNLOAD_TOKEN", "")
    if not expected or not secrets.compare_digest(token, expected):
        raise HTTPException(status_code=403, detail="Invalid or missing token.")
    if not os.path.exists(sqlite_file_name):
        raise HTTPException(status_code=404, detail="Database file not found.")
    filename = f"ceew_survey_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.db"
    return FileResponse(sqlite_file_name, media_type="application/octet-stream", filename=filename)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)