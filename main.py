from fastapi import FastAPI, Request, Form, Depends
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import SQLModel, Field, Session, create_engine
from typing import Optional, List
from datetime import datetime
import uvicorn
import os
import secrets

class IndustryProfile(SQLModel, table=True):
    __tablename__ = "industry_profiles"
    __table_args__ = {'extend_existing': True} 
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_name: str
    industry_type: str
    industry_type_other: Optional[str] = None
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
    total_units: int
    tech_staff_env_dept: int
    dept_looks_after_om: str
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
    stack_using_cems: Optional[str] = None
    stack_third_party: Optional[str] = None
    stack_frequency: Optional[str] = None
    effluent_using_ceqms: Optional[str] = None
    effluent_third_party: Optional[str] = None
    aaq_using_caaqms: Optional[str] = None
    aaq_third_party: Optional[str] = None
    aaq_frequency: Optional[str] = None

class CemsInstallation(SQLModel, table=True):
    __tablename__ = "cems_installations"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    unit_identifier: str
    cems_parameter: str
    emission_level: Optional[str] = None
    brand: Optional[str] = None
    technology: Optional[str] = None
    vendor: Optional[str] = None
    certification: Optional[str] = None
    insitu_extractive: Optional[str] = None
    install_year: Optional[int] = None
    location: Optional[str] = None
    position_8d2d: Optional[str] = None
    stratification: Optional[str] = None
    applicable_limit: Optional[str] = None
    monitoring_range: Optional[str] = None
    physical_maintenance: Optional[str] = None
    who_carries: Optional[str] = None
    frequency: Optional[str] = None

class CemsDriftGeneral(SQLModel, table=True):
    __tablename__ = "cems_drift_general"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    who_carries_drift: Optional[str] = None
    available_cylinders: Optional[str] = None
    cylinders_connected: Optional[str] = None

class CemsCalibrationCylinder(SQLModel, table=True):
    __tablename__ = "cems_calibration_cylinders"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    supplier: Optional[str] = None
    concentration: Optional[str] = None
    expiry_date: Optional[str] = None

class CemsDriftDetail(SQLModel, table=True):
    __tablename__ = "cems_drift_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    zero_drift_freq: Optional[str] = None
    zero_drift_time: Optional[str] = None
    zero_drift_process: Optional[str] = None
    span_drift_freq: Optional[str] = None
    span_drift_time: Optional[str] = None
    span_drift_process: Optional[str] = None

class CemsCalibrationPm(SQLModel, table=True):
    __tablename__ = "cems_calibration_pm"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    pm_cems_type: str
    tech_installed: Optional[str] = None
    calib_who: Optional[str] = None
    calib_tests: Optional[str] = None
    recalib_who: Optional[str] = None
    recalib_points: Optional[str] = None
    recalib_loads: Optional[str] = None
    recalib_interval: Optional[str] = None
    data_comp_who: Optional[str] = None
    data_comp_freq: Optional[str] = None
    data_comp_diff: Optional[str] = None

class CemsCalibrationGas(SQLModel, table=True):
    __tablename__ = "cems_calibration_gas"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    gas_cems_type: str
    cems_installed: Optional[str] = None
    calib_who: Optional[str] = None
    calib_tests: Optional[str] = None
    recalib_who: Optional[str] = None
    recalib_tests: Optional[str] = None
    recalib_freq: Optional[str] = None
    remote_avail: Optional[str] = None
    remote_carried: Optional[str] = None
    data_comp_who: Optional[str] = None
    data_comp_freq: Optional[str] = None

class CemsMaintenance(SQLModel, table=True):
    __tablename__ = "cems_maintenance"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    cems_type: str
    technology: Optional[str] = None
    activities: Optional[str] = None
    frequency: Optional[str] = None
    who_carries: Optional[str] = None
    spare_stock: Optional[str] = None
    contract: Optional[str] = None
    challenges: Optional[str] = None

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

class CeqmsInstallation(SQLModel, table=True):
    __tablename__ = "ceqms_installations"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    brand: Optional[str] = None
    technology: Optional[str] = None
    vendor: Optional[str] = None
    certification: Optional[str] = None
    installation_type: Optional[str] = None
    install_year: Optional[int] = None
    location: Optional[str] = None
    physical_maintenance: Optional[str] = None
    who_carries: Optional[str] = None
    frequency: Optional[str] = None

class CeqmsValidationGeneral(SQLModel, table=True):
    __tablename__ = "ceqms_validation_general"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    who_carries_validation: Optional[str] = None
    name_validation: Optional[str] = None
    who_carries_calibration: Optional[str] = None
    name_calibration: Optional[str] = None

class CeqmsValidationDetail(SQLModel, table=True):
    __tablename__ = "ceqms_validation_details"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    val_frequency: Optional[str] = None
    val_avg_diff: Optional[str] = None
    val_process: Optional[str] = None
    cal_frequency: Optional[str] = None
    cal_process: Optional[str] = None

class CeqmsMaintenance(SQLModel, table=True):
    __tablename__ = "ceqms_maintenance"
    __table_args__ = {'extend_existing': True}
    id: Optional[int] = Field(default=None, primary_key=True)
    industry_id: int = Field(foreign_key="industry_profiles.id")
    parameter: str
    activities: Optional[str] = None
    frequency: Optional[str] = None
    who_carries: Optional[str] = None
    spare_stock: Optional[str] = None
    contract: Optional[str] = None
    challenges: Optional[str] = None

class CeqmsImplementationLevel(SQLModel, table=True):
    __tablename__ = "ceqms_implementation_levels"
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
    challenge_1: Optional[str] = None
    challenge_2: Optional[str] = None
    challenge_3: Optional[str] = None
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

import os
DATA_DIR = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
sqlite_file_name = os.path.join(DATA_DIR, "database.db")
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=False)

def create_db_and_tables():
    SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

app = FastAPI()
templates = Jinja2Templates(directory="templates")

def render_template(name: str, context: dict):
    try:
        return templates.TemplateResponse(context["request"], name, context)
    except Exception:
        return templates.TemplateResponse(name, context)

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

@app.get("/", response_class=HTMLResponse)
async def show_form(request: Request):
    industry_types = ["Integrated cement making", "Coal-based power", "Distillery", "Pharmaceuticals", "Fertilizers", "Sugar", "Paper & Pulp", "Other"]
    states = ["Andaman and Nicobar Islands", "Andhra Pradesh", "Assam", "Bihar", "Chhattisgarh", "Delhi", "Goa", "Gujarat", "Haryana", "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Odisha", "Punjab", "Rajasthan", "Tamil Nadu", "Telangana", "Uttar Pradesh", "West Bengal"]
    return render_template("index.html", {"request": request, "industry_types": industry_types, "states": states})

@app.post("/submit_all")
async def submit_all(
    request: Request,
    industry_name: str = Form(...), industry_type: str = Form(...), industry_type_other: Optional[str] = Form(None), address: str = Form(...), district: str = Form(...), state: str = Form(...), contact_person: str = Form(...), designation: Optional[str] = Form(None), contact_number: str = Form(...), email: str = Form(...),
    total_units: int = Form(...), tech_staff_env_dept: int = Form(...), dept_looks_after_om: str = Form(...), inhouse_staff_number: int = Form(...), inhouse_responsibilities: str = Form(...), inhouse_training_attended: str = Form(...), vendor_staff_deployed: str = Form(...), vendor_name: Optional[str] = Form(None), vendor_responsibilities: Optional[str] = Form(None), vendor_training_attended: Optional[str] = Form(None), vendor_training_brief: Optional[str] = Form(None), vendor_visit_frequency: Optional[str] = Form(None),
    purchase_cems: Optional[float] = Form(None), purchase_ceqms: Optional[float] = Form(None), purchase_caaqms: Optional[float] = Form(None), purchase_total: Optional[float] = Form(None), om_cems: Optional[float] = Form(None), om_ceqms: Optional[float] = Form(None), om_caaqms: Optional[float] = Form(None), om_total: Optional[float] = Form(None), data_cems: Optional[float] = Form(None), data_ceqms: Optional[float] = Form(None), data_caaqms: Optional[float] = Form(None), data_total: Optional[float] = Form(None), lab_total: Optional[float] = Form(None),
    unit_number: List[str] = Form(None), name_operation: List[str] = Form(None), capacity_size: List[str] = Form(None), year_commissioning: List[str] = Form(None), stack_using_cems: List[str] = Form(None), stack_third_party: List[str] = Form(None), stack_frequency: List[str] = Form(None), effluent_using_ceqms: List[str] = Form(None), effluent_third_party: List[str] = Form(None), aaq_using_caaqms: List[str] = Form(None), aaq_third_party: List[str] = Form(None), aaq_frequency: List[str] = Form(None),
    
    cems_unit_id: List[str] = Form(None), cems_parameter: List[str] = Form(None), cems_emission_level: List[str] = Form(None), cems_brand: List[str] = Form(None), cems_technology: List[str] = Form(None), cems_vendor: List[str] = Form(None), cems_certification: List[str] = Form(None), cems_insitu_extractive: List[str] = Form(None), cems_install_year: List[str] = Form(None), cems_location: List[str] = Form(None), cems_position_8d2d: List[str] = Form(None), cems_stratification: List[str] = Form(None), cems_applicable_limit: List[str] = Form(None), cems_monitoring_range: List[str] = Form(None), cems_physical_maintenance: List[str] = Form(None), cems_who_carries: List[str] = Form(None), cems_frequency: List[str] = Form(None),
    drift_who_carries: Optional[str] = Form(None), drift_available_cylinders: Optional[str] = Form(None), drift_cylinders_connected: Optional[str] = Form(None), cyl_parameter: List[str] = Form(None), cyl_supplier: List[str] = Form(None), cyl_concentration: List[str] = Form(None), cyl_expiry: List[str] = Form(None), drift_parameter: List[str] = Form(None), drift_zero_freq: List[str] = Form(None), drift_zero_time: List[str] = Form(None), drift_zero_process: List[str] = Form(None), drift_span_freq: List[str] = Form(None), drift_span_time: List[str] = Form(None), drift_span_process: List[str] = Form(None),
    pm_calib_type: List[str] = Form(None), pm_tech: List[str] = Form(None), pm_calib_who: List[str] = Form(None), pm_calib_tests: List[str] = Form(None), pm_recalib_who: List[str] = Form(None), pm_recalib_pts: List[str] = Form(None), pm_recalib_loads: List[str] = Form(None), pm_recalib_int: List[str] = Form(None), pm_data_who: List[str] = Form(None), pm_data_freq: List[str] = Form(None), pm_data_diff: List[str] = Form(None),
    gas_calib_type: List[str] = Form(None), gas_cems_inst: List[str] = Form(None), gas_calib_who: List[str] = Form(None), gas_calib_tests: List[str] = Form(None), gas_recalib_who: List[str] = Form(None), gas_recalib_tests: List[str] = Form(None), gas_recalib_freq: List[str] = Form(None), gas_remote_avail: List[str] = Form(None), gas_remote_carried: List[str] = Form(None), gas_data_who: List[str] = Form(None), gas_data_freq: List[str] = Form(None),
    maint_cems_type: List[str] = Form(None), maint_tech: List[str] = Form(None), maint_activities: List[str] = Form(None), maint_frequency: List[str] = Form(None), maint_who: List[str] = Form(None), maint_stock: List[str] = Form(None), maint_contract: List[str] = Form(None), maint_challenges: List[str] = Form(None),
    impl_query: List[str] = Form(None), impl_confidence: List[str] = Form(None), impl_managed_by: List[str] = Form(None), impl_explanation: List[str] = Form(None), impl_guidance: List[str] = Form(None), impl_training: List[str] = Form(None),

    ceqms_param: List[str] = Form(None), ceqms_brand: List[str] = Form(None), ceqms_tech: List[str] = Form(None), ceqms_vendor: List[str] = Form(None), ceqms_cert: List[str] = Form(None), ceqms_install_type: List[str] = Form(None), ceqms_year: List[str] = Form(None), ceqms_loc: List[str] = Form(None), ceqms_maint: List[str] = Form(None), ceqms_who: List[str] = Form(None), ceqms_freq: List[str] = Form(None),
    ceqms_val_who: Optional[str] = Form(None), ceqms_val_name: Optional[str] = Form(None), ceqms_cal_who: Optional[str] = Form(None), ceqms_cal_name: Optional[str] = Form(None), ceqms_val_param: List[str] = Form(None), ceqms_val_freq: List[str] = Form(None), ceqms_val_diff: List[str] = Form(None), ceqms_val_process: List[str] = Form(None), ceqms_cal_freq: List[str] = Form(None), ceqms_cal_process: List[str] = Form(None),
    ceqms_maint_param: List[str] = Form(None), ceqms_maint_act: List[str] = Form(None), ceqms_maint_freq: List[str] = Form(None), ceqms_maint_who: List[str] = Form(None), ceqms_maint_stock: List[str] = Form(None), ceqms_maint_contract: List[str] = Form(None), ceqms_maint_challenges: List[str] = Form(None),
    ceqms_impl_query: List[str] = Form(None), ceqms_impl_confidence: List[str] = Form(None), ceqms_impl_managed_by: List[str] = Form(None), ceqms_impl_explanation: List[str] = Form(None), ceqms_impl_guidance: List[str] = Form(None), ceqms_impl_training: List[str] = Form(None),

    caaqms_inst_sys_id: List[str] = Form(None), caaqms_inst_monitor: List[str] = Form(None), caaqms_inst_brand: List[str] = Form(None), caaqms_inst_tech: List[str] = Form(None), caaqms_inst_vendor: List[str] = Form(None), caaqms_inst_cert: List[str] = Form(None), caaqms_inst_year: List[str] = Form(None), caaqms_inst_loc: List[str] = Form(None), caaqms_inst_maint: List[str] = Form(None), caaqms_inst_who: List[str] = Form(None), caaqms_inst_freq: List[str] = Form(None),
    caaqms_drift_who_carries: Optional[str] = Form(None), caaqms_drift_available_cylinders: Optional[str] = Form(None), caaqms_drift_cylinders_connected: Optional[str] = Form(None),
    caaqms_cyl_parameter: List[str] = Form(None), caaqms_cyl_supplier: List[str] = Form(None), caaqms_cyl_concentration: List[str] = Form(None), caaqms_cyl_expiry: List[str] = Form(None),
    caaqms_drift_parameter: List[str] = Form(None), caaqms_drift_zero_freq: List[str] = Form(None), caaqms_drift_zero_process: List[str] = Form(None), caaqms_drift_span_freq: List[str] = Form(None), caaqms_drift_span_process: List[str] = Form(None), caaqms_calib_freq: List[str] = Form(None), caaqms_calib_process: List[str] = Form(None),
    caaqms_maint_tech: List[str] = Form(None), caaqms_maint_act: List[str] = Form(None), caaqms_maint_freq: List[str] = Form(None), caaqms_maint_who: List[str] = Form(None), caaqms_maint_stock: List[str] = Form(None), caaqms_maint_contract: List[str] = Form(None), caaqms_maint_challenges: List[str] = Form(None),
    caaqms_impl_query: List[str] = Form(None), caaqms_impl_confidence: List[str] = Form(None), caaqms_impl_managed_by: List[str] = Form(None), caaqms_impl_explanation: List[str] = Form(None), caaqms_impl_guidance: List[str] = Form(None), caaqms_impl_training: List[str] = Form(None),

    challenge_phase: List[str] = Form(None), challenge_system: List[str] = Form(None), challenge_1: List[str] = Form(None), challenge_2: List[str] = Form(None), challenge_3: List[str] = Form(None), challenge_helped_most: List[str] = Form(None), challenge_how_helped: List[str] = Form(None), regulatory_support: List[str] = Form(None),
    suggestion_1: str = Form(...), suggestion_2: Optional[str] = Form(None), suggestion_3: Optional[str] = Form(None), suggestion_4: Optional[str] = Form(None), suggestion_5: Optional[str] = Form(None),
    session: Session = Depends(get_session)
):
    def int_or_none(lst, idx):
        if not lst or idx >= len(lst): return None
        v = str(lst[idx]).strip().upper()
        if v in ('', 'NA', 'N/A', 'NONE'): return None
        try: return int(v)
        except (ValueError, TypeError): return None

    new_profile = IndustryProfile(industry_name=industry_name, industry_type=industry_type, industry_type_other=industry_type_other, address=address, district=district, state=state, contact_person=contact_person, designation=designation, contact_number=contact_number, email=email)
    session.add(new_profile)
    session.commit()
    session.refresh(new_profile) 

    new_resource = ResourceAvailability(industry_id=new_profile.id, total_units=total_units, tech_staff_env_dept=tech_staff_env_dept, dept_looks_after_om=dept_looks_after_om, inhouse_staff_number=inhouse_staff_number, inhouse_responsibilities=inhouse_responsibilities, inhouse_training_attended=inhouse_training_attended, vendor_staff_deployed=vendor_staff_deployed, vendor_name=vendor_name, vendor_responsibilities=vendor_responsibilities, vendor_training_attended=vendor_training_attended, vendor_training_brief=vendor_training_brief, vendor_visit_frequency=vendor_visit_frequency)
    session.add(new_resource)
    
    new_expenses = ExpensesMonitoring(industry_id=new_profile.id, purchase_cems=purchase_cems, purchase_ceqms=purchase_ceqms, purchase_caaqms=purchase_caaqms, purchase_total=purchase_total, om_cems=om_cems, om_ceqms=om_ceqms, om_caaqms=om_caaqms, om_total=om_total, data_cems=data_cems, data_ceqms=data_ceqms, data_caaqms=data_caaqms, data_total=data_total, lab_total=lab_total)
    session.add(new_expenses)

    if unit_number and len(unit_number) > 0:
        for i in range(len(unit_number)):
            unit = UnitDetail(industry_id=new_profile.id, unit_number=unit_number[i], name_operation=name_operation[i] if len(name_operation)>i else None, capacity_size=capacity_size[i] if len(capacity_size)>i else None, year_commissioning=int_or_none(year_commissioning, i), stack_using_cems=stack_using_cems[i] if len(stack_using_cems)>i else None, stack_third_party=stack_third_party[i] if len(stack_third_party)>i else None, stack_frequency=stack_frequency[i] if len(stack_frequency)>i else None, effluent_using_ceqms=effluent_using_ceqms[i] if len(effluent_using_ceqms)>i else None, effluent_third_party=effluent_third_party[i] if len(effluent_third_party)>i else None, aaq_using_caaqms=aaq_using_caaqms[i] if len(aaq_using_caaqms)>i else None, aaq_third_party=aaq_third_party[i] if len(aaq_third_party)>i else None, aaq_frequency=aaq_frequency[i] if len(aaq_frequency)>i else None)
            session.add(unit)

    if cems_unit_id and len(cems_unit_id) > 0:
        for i in range(len(cems_unit_id)):
            if cems_parameter[i]: 
                cems_inst = CemsInstallation(industry_id=new_profile.id, unit_identifier=cems_unit_id[i], cems_parameter=cems_parameter[i], emission_level=cems_emission_level[i] if len(cems_emission_level)>i else None, brand=cems_brand[i] if len(cems_brand)>i else None, technology=cems_technology[i] if len(cems_technology)>i else None, vendor=cems_vendor[i] if len(cems_vendor)>i else None, certification=cems_certification[i] if len(cems_certification)>i else None, insitu_extractive=cems_insitu_extractive[i] if len(cems_insitu_extractive)>i else None, install_year=int_or_none(cems_install_year, i), location=cems_location[i] if len(cems_location)>i else None, position_8d2d=cems_position_8d2d[i] if len(cems_position_8d2d)>i else None, stratification=cems_stratification[i] if len(cems_stratification)>i else None, applicable_limit=cems_applicable_limit[i] if len(cems_applicable_limit)>i else None, monitoring_range=cems_monitoring_range[i] if len(cems_monitoring_range)>i else None, physical_maintenance=cems_physical_maintenance[i] if len(cems_physical_maintenance)>i else None, who_carries=cems_who_carries[i] if len(cems_who_carries)>i else None, frequency=cems_frequency[i] if len(cems_frequency)>i else None)
                session.add(cems_inst)
        drift_general = CemsDriftGeneral(industry_id=new_profile.id, who_carries_drift=drift_who_carries, available_cylinders=drift_available_cylinders, cylinders_connected=drift_cylinders_connected)
        session.add(drift_general)
        if cyl_parameter:
            for i in range(len(cyl_parameter)):
                cyl = CemsCalibrationCylinder(industry_id=new_profile.id, parameter=cyl_parameter[i], supplier=cyl_supplier[i] if len(cyl_supplier)>i else None, concentration=cyl_concentration[i] if len(cyl_concentration)>i else None, expiry_date=cyl_expiry[i] if len(cyl_expiry)>i else None)
                session.add(cyl)
        if drift_parameter:
            for i in range(len(drift_parameter)):
                drift_det = CemsDriftDetail(industry_id=new_profile.id, parameter=drift_parameter[i], zero_drift_freq=drift_zero_freq[i] if len(drift_zero_freq)>i else None, zero_drift_time=drift_zero_time[i] if len(drift_zero_time)>i else None, zero_drift_process=drift_zero_process[i] if len(drift_zero_process)>i else None, span_drift_freq=drift_span_freq[i] if len(drift_span_freq)>i else None, span_drift_time=drift_span_time[i] if len(drift_span_time)>i else None, span_drift_process=drift_span_process[i] if len(drift_span_process)>i else None)
                session.add(drift_det)
        if pm_calib_type:
            for i in range(len(pm_calib_type)):
                pm_cal = CemsCalibrationPm(industry_id=new_profile.id, pm_cems_type=pm_calib_type[i], tech_installed=pm_tech[i] if len(pm_tech)>i else None, calib_who=pm_calib_who[i] if len(pm_calib_who)>i else None, calib_tests=pm_calib_tests[i] if len(pm_calib_tests)>i else None, recalib_who=pm_recalib_who[i] if len(pm_recalib_who)>i else None, recalib_points=pm_recalib_pts[i] if len(pm_recalib_pts)>i else None, recalib_loads=pm_recalib_loads[i] if len(pm_recalib_loads)>i else None, recalib_interval=pm_recalib_int[i] if len(pm_recalib_int)>i else None, data_comp_who=pm_data_who[i] if len(pm_data_who)>i else None, data_comp_freq=pm_data_freq[i] if len(pm_data_freq)>i else None, data_comp_diff=pm_data_diff[i] if len(pm_data_diff)>i else None)
                session.add(pm_cal)
        if gas_calib_type:
            for i in range(len(gas_calib_type)):
                gas_cal = CemsCalibrationGas(industry_id=new_profile.id, gas_cems_type=gas_calib_type[i], cems_installed=gas_cems_inst[i] if len(gas_cems_inst)>i else None, calib_who=gas_calib_who[i] if len(gas_calib_who)>i else None, calib_tests=gas_calib_tests[i] if len(gas_calib_tests)>i else None, recalib_who=gas_recalib_who[i] if len(gas_recalib_who)>i else None, recalib_tests=gas_recalib_tests[i] if len(gas_recalib_tests)>i else None, recalib_freq=gas_recalib_freq[i] if len(gas_recalib_freq)>i else None, remote_avail=gas_remote_avail[i] if len(gas_remote_avail)>i else None, remote_carried=gas_remote_carried[i] if len(gas_remote_carried)>i else None, data_comp_who=gas_data_who[i] if len(gas_data_who)>i else None, data_comp_freq=gas_data_freq[i] if len(gas_data_freq)>i else None)
                session.add(gas_cal)
        if maint_cems_type:
            for i in range(len(maint_cems_type)):
                maint = CemsMaintenance(industry_id=new_profile.id, cems_type=maint_cems_type[i], technology=maint_tech[i] if len(maint_tech)>i else None, activities=maint_activities[i] if len(maint_activities)>i else None, frequency=maint_frequency[i] if len(maint_frequency)>i else None, who_carries=maint_who[i] if len(maint_who)>i else None, spare_stock=maint_stock[i] if len(maint_stock)>i else None, contract=maint_contract[i] if len(maint_contract)>i else None, challenges=maint_challenges[i] if len(maint_challenges)>i else None)
                session.add(maint)
        if impl_query:
            for i in range(len(impl_query)):
                impl = CemsImplementationLevel(industry_id=new_profile.id, query_text=impl_query[i], confidence=impl_confidence[i] if len(impl_confidence)>i else None, managed_by=impl_managed_by[i] if len(impl_managed_by)>i else None, explanation=impl_explanation[i] if len(impl_explanation)>i else None, independent_guidance=impl_guidance[i] if len(impl_guidance)>i else None, training_preference=impl_training[i] if len(impl_training)>i else None)
                session.add(impl)

    if ceqms_param and len(ceqms_param) > 0:
        for i in range(len(ceqms_param)):
            if ceqms_param[i].strip() != "":
                ceqms = CeqmsInstallation(industry_id=new_profile.id, parameter=ceqms_param[i], brand=ceqms_brand[i] if len(ceqms_brand)>i else None, technology=ceqms_tech[i] if len(ceqms_tech)>i else None, vendor=ceqms_vendor[i] if len(ceqms_vendor)>i else None, certification=ceqms_cert[i] if len(ceqms_cert)>i else None, installation_type=ceqms_install_type[i] if len(ceqms_install_type)>i else None, install_year=int_or_none(ceqms_year, i), location=ceqms_loc[i] if len(ceqms_loc)>i else None, physical_maintenance=ceqms_maint[i] if len(ceqms_maint)>i else None, who_carries=ceqms_who[i] if len(ceqms_who)>i else None, frequency=ceqms_freq[i] if len(ceqms_freq)>i else None)
                session.add(ceqms)
        ceqms_val_gen = CeqmsValidationGeneral(industry_id=new_profile.id, who_carries_validation=ceqms_val_who, name_validation=ceqms_val_name, who_carries_calibration=ceqms_cal_who, name_calibration=ceqms_cal_name)
        session.add(ceqms_val_gen)
        if ceqms_val_param:
            for i in range(len(ceqms_val_param)):
                ceqms_val = CeqmsValidationDetail(industry_id=new_profile.id, parameter=ceqms_val_param[i], val_frequency=ceqms_val_freq[i] if len(ceqms_val_freq)>i else None, val_avg_diff=ceqms_val_diff[i] if len(ceqms_val_diff)>i else None, val_process=ceqms_val_process[i] if len(ceqms_val_process)>i else None, cal_frequency=ceqms_cal_freq[i] if len(ceqms_cal_freq)>i else None, cal_process=ceqms_cal_process[i] if len(ceqms_cal_process)>i else None)
                session.add(ceqms_val)
        if ceqms_maint_param:
            for i in range(len(ceqms_maint_param)):
                if ceqms_maint_param[i].strip() != "":
                    c_maint = CeqmsMaintenance(industry_id=new_profile.id, parameter=ceqms_maint_param[i], activities=ceqms_maint_act[i] if len(ceqms_maint_act)>i else None, frequency=ceqms_maint_freq[i] if len(ceqms_maint_freq)>i else None, who_carries=ceqms_maint_who[i] if len(ceqms_maint_who)>i else None, spare_stock=ceqms_maint_stock[i] if len(ceqms_maint_stock)>i else None, contract=ceqms_maint_contract[i] if len(ceqms_maint_contract)>i else None, challenges=ceqms_maint_challenges[i] if len(ceqms_maint_challenges)>i else None)
                    session.add(c_maint)
        if ceqms_impl_query:
            for i in range(len(ceqms_impl_query)):
                c_impl = CeqmsImplementationLevel(industry_id=new_profile.id, query_text=ceqms_impl_query[i], confidence=ceqms_impl_confidence[i] if len(ceqms_impl_confidence)>i else None, managed_by=ceqms_impl_managed_by[i] if len(ceqms_impl_managed_by)>i else None, explanation=ceqms_impl_explanation[i] if len(ceqms_impl_explanation)>i else None, independent_guidance=ceqms_impl_guidance[i] if len(ceqms_impl_guidance)>i else None, training_preference=ceqms_impl_training[i] if len(ceqms_impl_training)>i else None)
                session.add(c_impl)

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
            challenge = ImplementationChallenge(industry_id=new_profile.id, phase=challenge_phase[i], system_type=challenge_system[i], challenge_1=challenge_1[i] if challenge_1 and len(challenge_1) > i else None, challenge_2=challenge_2[i] if challenge_2 and len(challenge_2) > i else None, challenge_3=challenge_3[i] if challenge_3 and len(challenge_3) > i else None, helped_most=challenge_helped_most[i] if challenge_helped_most and len(challenge_helped_most) > i else None, how_it_helped=challenge_how_helped[i] if challenge_how_helped and len(challenge_helped_most) > i else None, regulatory_support=regulatory_support[i] if regulatory_support and len(regulatory_support) > i else None)
            session.add(challenge)

    new_improvement = ExpectedImprovement(industry_id=new_profile.id, suggestion_1=suggestion_1, suggestion_2=suggestion_2, suggestion_3=suggestion_3, suggestion_4=suggestion_4, suggestion_5=suggestion_5)
    session.add(new_improvement)

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
    """Password-protected endpoint to download the SQLite database file.
    Set the DB_DOWNLOAD_TOKEN environment variable to enable this.
    Usage: GET /download-db?token=your_secret_token
    """
    expected = os.environ.get("DB_DOWNLOAD_TOKEN", "")
    if not expected or not secrets.compare_digest(token, expected):
        from fastapi import HTTPException
        raise HTTPException(status_code=403, detail="Invalid or missing token.")
    if not os.path.exists(sqlite_file_name):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Database file not found.")
    filename = f"ceew_survey_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.db"
    return FileResponse(sqlite_file_name, media_type="application/octet-stream", filename=filename)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)