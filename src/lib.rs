use libc::c_int;
use pg_sys::Datum;
use pg_sys::*;
use pgrx::prelude::*;
use std::ffi::CString;
use std::ptr::null_mut;

pgrx::pg_module_magic!();

#[repr(i32)]
enum DummyAmEnum {
    DummyAmEnumOne,
    DummyAmEnumTwo,
}

#[repr(C)]
/* Dummy index options */
struct DummyIndexOptions {
    vl_len_: c_int, /* varlena header (do not touch directly!) */
    option_int: c_int,
    option_real: f64,
    option_bool: bool,
    option_enum: DummyAmEnum,
    option_string_val_offset: c_int,
    option_string_null_offset: c_int,
}

#[pgrx::pg_extern(sql = "
    CREATE OR REPLACE FUNCTION dummy_index_amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD zombodb TYPE INDEX HANDLER dummy_index_amhandler;
")]
fn dummy_index_amhandler(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> pgrx::PgBox<pgrx::pg_sys::IndexAmRoutine> {
    let mut am_routine = unsafe {
        pgrx::PgBox::<pgrx::pg_sys::IndexAmRoutine>::alloc_node(
            pgrx::pg_sys::NodeTag_T_IndexAmRoutine,
        )
    };

    am_routine.amstrategies = 0;
    am_routine.amsupport = 1;
    am_routine.amoptsprocnum = 0;

    am_routine.amcanorder = false;
    am_routine.amcanorderbyop = false;
    am_routine.amcanbackward = false;
    am_routine.amcanunique = false;
    am_routine.amcanmulticol = false;
    am_routine.amoptionalkey = false;
    am_routine.amsearcharray = false;
    am_routine.amsearchnulls = false;
    am_routine.amstorage = false;
    am_routine.amclusterable = false;
    am_routine.ampredlocks = false;
    am_routine.amcanparallel = false;
    am_routine.amcaninclude = false;
    am_routine.amusemaintenanceworkmem = false;
    am_routine.amkeytype = pgrx::pg_sys::InvalidOid;
    am_routine.amparallelvacuumoptions = 0;

    am_routine.ambuild = Some(dibuild);
    am_routine.ambuildempty = Some(dibuildempty);
    am_routine.aminsert = Some(diinsert);
    am_routine.ambulkdelete = Some(dibulkdelete);
    am_routine.amvacuumcleanup = Some(divacuumcleanup);

    am_routine.amcanreturn = None;

    am_routine.amcostestimate = Some(dicostestimate);
    am_routine.amoptions = Some(dioptions);

    am_routine.amproperty = None;
    am_routine.ambuildphasename = None;

    am_routine.amvalidate = Some(divalidate);
    am_routine.ambeginscan = Some(dibeginscan);
    am_routine.amrescan = Some(direscan);

    am_routine.amgettuple = None;
    am_routine.amgetbitmap = None;

    am_routine.amendscan = Some(diendscan);

    am_routine.ammarkpos = None;
    am_routine.amrestrpos = None;
    am_routine.amestimateparallelscan = None;
    am_routine.aminitparallelscan = None;
    am_routine.amparallelrescan = None;

    am_routine.into_pg_boxed()
}

#[pg_guard]
unsafe extern "C" fn dibuild(
    _heap: pg_sys::Relation,
    _index: pg_sys::Relation,
    _index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let mut index_build_result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    index_build_result.heap_tuples = 0.0;
    index_build_result.index_tuples = 0.0;
    index_build_result.into_pg()
}

#[pg_guard]
unsafe extern "C" fn dibuildempty(_relation: pg_sys::Relation) {}

#[pg_guard]
unsafe extern "C" fn diinsert(
    _index_relation: Relation,
    _values: *mut Datum,
    _isnull: *mut bool,
    _heap_tid: ItemPointer,
    _heap_relation: Relation,
    _check_unique: IndexUniqueCheck,
    _index_info: *mut IndexInfo,
) -> bool {
    false
}

#[pg_guard]
unsafe extern "C" fn dibulkdelete(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    null_mut()
}

#[pg_guard]
unsafe extern "C" fn divacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    null_mut()
}

#[pg_guard]
unsafe extern "C" fn dicostestimate(
    _root: *mut PlannerInfo,
    _path: *mut IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut Cost,
    index_total_cost: *mut Cost,
    index_selectivity: *mut Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    *index_startup_cost = 1e10;
    *index_total_cost = 1e10;
    *index_selectivity = 1.0;
    *index_correlation = 1.0;
    *index_pages = 1.0;
}

unsafe fn str_to_pchar(string: &str) -> CString {
    CString::new(string).unwrap()
}

unsafe extern "C" fn validate_string_option(string: *const i8) {
    panic!("Invalid string option {:?}", string);
}

fn create_reloptions_table() -> (u32, [pg_sys::relopt_parse_elt; 6]) {
    unsafe {
        let one = str_to_pchar("One").as_ptr();
        let two = str_to_pchar("Two").as_ptr();
        let mut dummy_am_enum_values: [pg_sys::relopt_enum_elt_def; 2] = [
            pg_sys::relopt_enum_elt_def {
                string_val: one,
                symbol_val: DummyAmEnum::DummyAmEnumOne as i32,
            },
            pg_sys::relopt_enum_elt_def {
                string_val: two,
                symbol_val: DummyAmEnum::DummyAmEnumTwo as i32,
            },
        ];

        let di_relopt_kind = add_reloption_kind();
        let mut di_relopt_tab: [pg_sys::relopt_parse_elt; 6] =
            [pg_sys::relopt_parse_elt::default(); 6];
        add_int_reloption(
            di_relopt_kind,
            str_to_pchar("option_int").as_ptr(),
            str_to_pchar("Integer option for dummy_index_am").as_ptr(),
            10,
            -10,
            100,
            AccessExclusiveLock as i32,
        );
        di_relopt_tab[0].optname = str_to_pchar("option_int").as_ptr();
        di_relopt_tab[0].opttype = 1;
        let dummy = std::ptr::null::<DummyIndexOptions>();
        di_relopt_tab[0].offset = &(*dummy).option_int as *const _ as i32;
        add_real_reloption(
            di_relopt_kind,
            str_to_pchar("option_real").as_ptr(),
            str_to_pchar("Real option for dummy_index_am").as_ptr(),
            3.1415,
            -10.0,
            100.0,
            AccessExclusiveLock as i32,
        );
        di_relopt_tab[1].optname = str_to_pchar("option_real").as_ptr();
        di_relopt_tab[1].opttype = 2;
        di_relopt_tab[1].offset = &(*dummy).option_real as *const _ as i32;
        add_bool_reloption(
            di_relopt_kind,
            str_to_pchar("option_bool").as_ptr(),
            str_to_pchar("Bool option for dummy_index_am").as_ptr(),
            true,
            AccessExclusiveLock as i32,
        );
        di_relopt_tab[2].optname = str_to_pchar("option_bool").as_ptr();
        di_relopt_tab[2].opttype = 0;
        di_relopt_tab[2].offset = &(*dummy).option_bool as *const _ as i32;
        add_enum_reloption(
            di_relopt_kind,
            str_to_pchar("option_enum").as_ptr(),
            str_to_pchar("Enum option for dummy_index_am").as_ptr(),
            dummy_am_enum_values.as_mut_ptr(),
            DummyAmEnum::DummyAmEnumOne as i32,
            str_to_pchar("Valid values are \"one\" and \"two\".").as_ptr(),
            AccessExclusiveLock as i32,
        );
        di_relopt_tab[3].optname = str_to_pchar("option_enum").as_ptr();
        di_relopt_tab[3].opttype = 3;
        di_relopt_tab[3].offset = &(*dummy).option_enum as *const _ as i32;
        add_string_reloption(
            di_relopt_kind,
            str_to_pchar("option_string_val").as_ptr(),
            str_to_pchar("String option for dummy_index_am").as_ptr(),
            str_to_pchar("DefaultValue").as_ptr(),
            Some(validate_string_option),
            AccessExclusiveLock as i32,
        );
        di_relopt_tab[4].optname = str_to_pchar("option_string_val").as_ptr();
        di_relopt_tab[4].opttype = 4;
        di_relopt_tab[4].offset = &(*dummy).option_string_val_offset as *const _ as i32;
        add_string_reloption(
            di_relopt_kind,
            str_to_pchar("option_string_null").as_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            Some(validate_string_option),
            AccessExclusiveLock as i32,
        );
        di_relopt_tab[5].optname = str_to_pchar("option_string_null").as_ptr();
        di_relopt_tab[5].opttype = 4;
        di_relopt_tab[5].offset = &(*dummy).option_string_null_offset as *const _ as i32;
        (di_relopt_kind, di_relopt_tab)
    }
}

#[pg_guard]
unsafe extern "C" fn dioptions(reloptions: Datum, validate: bool) -> *mut bytea {
    let (di_relopt_kind, di_relopt_tab) = create_reloptions_table();
    build_reloptions(
        reloptions,
        validate,
        di_relopt_kind,
        std::mem::size_of::<DummyIndexOptions>(),
        di_relopt_tab.as_ptr(),
        std::mem::size_of::<[pg_sys::relopt_parse_elt; 6]>() as i32,
    ) as *mut bytea
}

#[pg_guard]
unsafe extern "C" fn divalidate(_opclassoid: Oid) -> bool {
    true
}

#[pg_guard]
unsafe extern "C" fn dibeginscan(
    index_relation: Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> IndexScanDesc {
    let scan: PgBox<IndexScanDescData> =
        unsafe { PgBox::from_pg(RelationGetIndexScan(index_relation, nkeys, norderbys)) };
    scan.into_pg()
}

#[pg_guard]
unsafe extern "C" fn direscan(
    _scan: IndexScanDesc,
    _keys: ScanKey,
    _nkeys: ::std::os::raw::c_int,
    _orderbys: ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
}

#[pg_guard]
unsafe extern "C" fn diendscan(_scan: IndexScanDesc) {}


#[allow(non_snake_case)]
#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
}