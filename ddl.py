from oauth2client.service_account import ServiceAccountCredentials
import gspread
import db_connect

def ddl():
    #database connection
    connection = db_connect.get_connection()
    cur = connection.cursor()

    #googlesheets connection
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('client_services.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open('High Value Acq Targets: All Markets')
    #spreadsheet = client.open('property_scraper')
    worksheet = spreadsheet.worksheet("All_properties")

    #clear worksheet for daily dump
    worksheet.clear()

    #allProperties to database and googlesheets
    allFields = [
        'unique_id',
        'address',
        'city',
        'zip_code',
        'price',
        'orig_list_price',
        'status',
        'days_on_market',
        'total_days_on_market',
        'current_price_per_sqft',
        'num_beds',
        'num_baths',
        'numberof_full_baths',
        'numberof_half_baths',
        'year_built',
        'living_sq_feet',
        'groundfloor_sq_feet',
        'sqft_finished',
        'total_sqft',
        'interior_sq_ft',
        'building_sqft',
        'heated_area',
        'amenities_num_stories',
        'num_stories',
        'num_parking_spaces',
        'numof_garage_spaces',
        'lot_sqft',
        'lot_acreage',
        'lot_size',
        'elementaryS_name',
        'highS_name',
        'subdivision_name',
        'has_hoa',
        'has_hoa_leasing_restrictions',
        'hoa_fee_usd',
        'hoa_frequency',
        'hoa_rules',
        'pre_uw_annual_hoa_fee_est_usd',
        'association_yes_or_no',
        'association_fee',
        'association_fee_frequency',
        'hoa_rent_restrictions',
        'master_association_fee',
        'hoa_dues',
        'hoa_mandatory',
        'maint_fee_amt',
        'maint_fee_pay_schedule',
        'restrictions',
        'other_mandatory_fee',
        'fee_other_amount',
        'fee_other',
        'hoa_fee_requirement',
        'taxes_due',
        'pre_uw_annual_property_tax_est_usd',
        'tax_amount',
        'scraper_source',
        'mlsNumber_or_id',
        'created_at',
        'updated_at',
        'resurface_reason',
        'bus_station_distance',
        'bus_station_duration',
        'subway_station_distance',
        'subway_station_duration',
        'basement_sq_feet',
        'basement_type',
        'amenities_stories_type',
        'parking_type',
        'garage_type',
        'sewer_type',
        'water_type',
        'flood_zone',
        'taxes_rollYear',
        'history_marketing_remarks',
        'elementaryS_rating',
        'elementaryS_distance',
        'middleS_name',
        'middleS_rating',
        'middleS_distance',
        'highS_rating',
        'highS_distance',
        'rental_estimate_low',
        'rental_estimate_high',
        'state',
        'county',
        'latitude',
        'longitude',
        'roof',
        'construction',
        'has_well',
        'solar_panels',
        'use_code',
        'apn',
        'pool',
        'legal_desc',
        'rental_avm_value',
        'rental_avm_high',
        'rental_avm_low',
        'gross_yield',
        'hoa_amount',
        'images_url',
        'details_url',
        'offer_url',
        'change_date',
        'auction_end_time',
        'auction_start_time',
        'dwelling_type',
        'estimated_current_yield',
        'flooring_type',
        'floors',
        'has_addition',
        'has_garage_conversion',
        'has_known_foundation_issues',
        'kitchen_appliance_type',
        'kitchen_countertop',
        'opendoor_brokerage_pre_underwritten',
        'pre_uw_annual_insurance_expense_est_usd',
        'pre_uw_annual_ongoing_repair_expense_est_usd',
        'pre_uw_annual_property_mgmt_fee_est_usd',
        'pre_uw_annual_rent_amount_high_est_usd',
        'pre_uw_annual_rent_amount_low_est_usd',
        'pre_uw_annual_rent_amount_mid_est_usd',
        'pre_uw_annual_vacancy_expense_est_usd',
        'pre_uw_cap_rate_high_est',
        'pre_uw_cap_rate_low_est',
        'pre_uw_cap_rate_mid_est',
        'pre_uw_home_valuation_high_est_usd',
        'pre_uw_home_valuation_low_est_usd',
        'pre_uw_home_valuation_mid_est_usd',
        'pre_uw_repair_amount_high_est_usd',
        'pre_uw_repair_amount_low_est_usd',
        'pre_uw_repair_amount_mid_est_usd',
        'product_type',
        'sherlock_assessment',
        'suggested_offer',
        'carport_spaces',
        'block',
        'area',
        'structure_type',
        'sold_price',
        'closed_date',
        'zestimate',
        'rentzestimate',
        'property_estimate',
        'original_date'
    ]
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.all_properties (
            {','.join(f'{field} text' for field in allFields)}
        );
    """)
    connection.commit()
    worksheet.append_row(allFields)
    worksheet.format('A1:GA1', {'textFormat': {'bold': True}})

    #Offerpad to database
    OFFfields = [
        'id',
        'starting_price',
        'created_at'
    ]   
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.offerpad_properties (
            {','.join(f'{field} text' for field in OFFfields)}
        );
    """)
    connection.commit()

    #Opendoor to database
    OPENfields = [
        'id',
        'guidance_price',
        'created_at'
    ]   
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.opendoor_properties (
            {','.join(f'{field} text' for field in OPENfields)}
        );
    """)
    connection.commit()

    #FMLS to database
    FMLSfields = [
        'mls_number',
        'price',
        'total_days_on_market',
        'created_at',
        'status'
    ]
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.fmls_properties (
            {','.join(f'{field} text' for field in FMLSfields)}
        );
    """)
    connection.commit()

    #NTREIS to database
    NTfields = [
        'mls_number',
        'current_price',
        'cdom',
        'created_at',
        'status'
    ]   
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.ntreismls_properties (
            {','.join(f'{field} text' for field in NTfields)}
        );
    """)
    connection.commit()

    #HARMLS to database
    HARfields = [
        'mls_number',
        'current_price',
        'cdom',
        'created_at',
        'status'
    ]
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.harmls_properties (
            {','.join(f'{field} text' for field in HARfields)}
        );
    """)
    connection.commit()

    #FLEXMLS to database
    FLEXfields = [
        'listing_number',
        'price',
        'cumulative_dom',
        'created_at',
        'status'
    ]
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.flexmls_properties (
            {','.join(f'{field} text' for field in FLEXfields)}
        );
    """)
    connection.commit()

    #GeorgiaMLS to database
    GEOfields = [
        'listing_id',
        'list_price',
        'days_on_markets',
        'created_at',
        'status'
    ]
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.georgiamls_properties (
            {','.join(f'{field} text' for field in GEOfields)}
        );
    """)
    connection.commit()
    
    #TampaMLS to database
    Tampafields = [
        'mls_number',
        'current_price',
        'cdom',
        'created_at',
        'status'
    ]
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS ort;
        create table if not exists ort.tampamls_properties (
            {','.join(f'{field} text' for field in Tampafields)}
        );
    """)
    connection.commit()
    #close database connection
    connection.close()
ddl()