from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from datetime import date
import db_connect
import requests
import gspread
import apis

#Opendoor (Atlanta, Houston, Dallas, Jacksonville)
def getResponse():
    api_token = 'YOURAPITOKEN'
    base_url = 'https://directaccess.opendoor.com/api'
    datesdict={'unique_id':"",'created_at':"",'updated_at':"",'resurface_reason':""}

    response = requests.post(
            base_url + '/v2/leads',
            headers={'Accept': 'application/json', 'show': 'all', 'Authorization': api_token}
    )
    load = response.json()
    allproperties = load['leads']
    properties=list()
    for data in allproperties:
        market= data['market_name']
        if market=="atlanta" or market=="houston" or market=="dallas" or market=="jacksonville":
            temp = {}
            temp.update(data)
            temp.update(datesdict)
            properties.append(temp)
    return properties

def newRecControl():
    properties=getResponse()
    connection = db_connect.get_connection()
    cur = connection.cursor()
    cur.execute("SELECT id,guidance_price,created_at from ort.opendoor_properties")
    rows = cur.fetchall()
    cur.execute("SELECT unique_id from ort.all_properties")
    uniqs = cur.fetchall()
    if len(uniqs)==0:
        unique_id=1
    elif len(uniqs)>0:
        uniqids=[]
        for uniq in uniqs:
            uniqids.append(int(uniq[0]))
        unique_id = int(max(uniqids)) +1
    today = date.today().strftime("%m/%d/%Y")
    refuses=list()
    
    # rows are from db, properties are from scraper
    for row in rows:          
        temp=0   
        cdate = datetime.strptime(str(row[2]), '%m/%d/%Y') 
        td = datetime.strptime(today, '%m/%d/%Y') 
        daysnum=(td-cdate).days        
        if int(daysnum) == 45 or int(daysnum) == 90:
            temp=1  
        for prop in properties:
            proID = str(prop['id'])
            newPrice=0
            if 'guidance_price' in prop:
                newPrice=prop['guidance_price'] 
            if proID == str(row[0]):
                createdDate=row[2]
                oldPrice = float(str(row[1]).replace("$",""))
                if createdDate == today:   
                    if int(newPrice) < oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price reduction"
                        refuses.append(prop)
                    elif int(newPrice) > oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price increase"
                        refuses.append(prop)
                    del properties[properties.index(prop)]
                else:
                    if int(newPrice) < oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price reduction"
                        refuses.append(prop)
                        del properties[properties.index(prop)]
                    elif int(newPrice) > oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price increase"
                        refuses.append(prop)
                        del properties[properties.index(prop)]
                    elif temp==1:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="more than 45 days or 90 days"
                        refuses.append(prop)                      
                        del properties[properties.index(prop)]
    #Filtering
    masterdata=[]
    for d in properties:
        if float(d['guidance_price'])>=100000 and float(d['guidance_price'])<=270000:
            if float(d['bedrooms'])>2 and float(d['year_built']) > 1940:
                Isdistance = apis.calculateDistance(d['street_address'])
                if Isdistance == 1:          
                    masterdata.append(d)
    for line in masterdata:
        line['created_at']=today                  
    for line in refuses:
        masterdata.append(line)
    for line in masterdata:
        line['unique_id']=unique_id
        unique_id = unique_id+1  
    connection.close()     
    return masterdata  

def insert_data():
    #Connecting to database 
    connection = db_connect.get_connection()
    cur = connection.cursor()

    #Connecting to googlesheets
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name('client_services.json', scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open('High Value Acq Targets: All Markets')
    #spreadsheet = client.open('property_scraper')
    worksheet = spreadsheet.worksheet("All_properties")

    properties = newRecControl()
    stationTypes=['bus stop','subway stop']
    for line in properties:
        allFieldsdict={'unique_id':"",'address':"",'city':"",'zip_code':"",'price':"",'orig_list_price':"",'status':"",'days_on_market':"",'total_days_on_market':"",'current_price_per_sqft':"",'num_beds':"",'num_baths':"",'numberof_full_baths':"",'numberof_half_baths':"",'year_built':"",'living_sq_feet':"",
        'groundfloor_sq_feet':"",'sqft_finished':"",'total_sqft':"",'interior_sq_ft':"",'building_sqft':"",'heated_area':"",'amenities_num_stories':"",'num_stories':"",'num_parking_spaces':"",'numof_garage_spaces':"",'lot_sqft':"",'lot_acreage':"",'lot_size':"",'elementaryS_name':"",'highS_name':"",
        'subdivision_name':"",'has_hoa':"",'has_hoa_leasing_restrictions':"",'hoa_fee_usd':"",'hoa_frequency':"",'hoa_rules':"",'pre_uw_annual_hoa_fee_est_usd':"",'association_yes_or_no':"",'association_fee':"",'association_fee_frequency':"",'hoa_rent_restrictions':"",'master_association_fee':"",'hoa_dues':"",
        'hoa_mandatory':"",'maint_fee_amt':"",'maint_fee_pay_schedule':"",'restrictions':"",'other_mandatory_fee':"",'fee_other_amount':"",'fee_other':"",'hoa_fee_requirement':"",'taxes_due':"",'pre_uw_annual_property_tax_est_usd':"",'tax_amount':"",'scrape_source':"",'mlsNumber_or_id':"",
        'created_at':"",'updated_at':"",'resurface_reason':"",'bus_station_distance':"",'bus_station_duration':"",'subway_station_distance':"",'subway_station_duration':"",'basement_sq_feet':"",'basement_type':"",'amenities_stories_type':"",'parking_type':"",'garage_type':"",'sewer_type':"",'water_type':"",
        'flood_zone':"",'taxes_rollYear':"",'history_marketing_remarks':"",'elementaryS_rating':"",'elementaryS_distance':"",'middleS_name':"",'middleS_rating':"",'middleS_distance':"",'highS_rating':"",'highS_distance':"",'rental_estimate_low':"",'rental_estimate_high':"",'state':"",'county':"",'latitude':"",
        'longitude':"",'roof':"",'construction':"",'has_well':"",'solar_panels':"",'use_code':"",'apn':"",'pool':"",'legal_desc':"",'rental_avm_value':"",'rental_avm_high':"",'rental_avm_low':"",'gross_yield':"",'hoa_amount':"",'images_url':"",'details_url':"",'offer_url':"",'change_date':"",
        'auction_end_time':"",'auction_start_time':"",'dwelling_type':"",'estimated_current_yield':"",'flooring_type':"",'floors':"",'has_addition':"",'has_garage_conversion':"",'has_known_foundation_issues':"",'kitchen_appliance_type':"",'kitchen_countertop':"",'opendoor_brokerage_pre_underwritten':"",
        'pre_uw_annual_insurance_expense_est_usd':"",'pre_uw_annual_ongoing_repair_expense_est_usd':"",'pre_uw_annual_property_mgmt_fee_est_usd':"",'pre_uw_annual_rent_amount_high_est_usd':"",'pre_uw_annual_rent_amount_low_est_usd':"",'pre_uw_annual_rent_amount_mid_est_usd':"",'pre_uw_annual_vacancy_expense_est_usd':"",'pre_uw_cap_rate_high_est':"",
        'pre_uw_cap_rate_low_est':"",'pre_uw_cap_rate_mid_est':"",'pre_uw_home_valuation_high_est_usd':"",'pre_uw_home_valuation_low_est_usd':"",'pre_uw_home_valuation_mid_est_usd':"",'pre_uw_repair_amount_high_est_usd':"",'pre_uw_repair_amount_low_est_usd':"",'pre_uw_repair_amount_mid_est_usd':"",'product_type':"",'sherlock_assessment':"",
        'suggested_offer':"",'carport_spaces':"",'block':"",'area':"",'structure_type':"",'sold_price':"",'closed_date':"",'zestimate':"",'rentzestimate':"",
        'property_estimate':"",'original_date':""}
        address = line['street_address']
        zip = line['zip_code']
        location = address + ", " +zip
        #google
        timeVal=str()
        dist=str()
        for type in stationTypes:
            retVal = apis.closestStation(location,type)     
            if retVal != 'ZERO_RESULTS' :    
                mins = retVal.split(",")
                timeValarr=mins[0].split(" ")
                if len(timeValarr)==2:
                    timeVal=int(timeValarr[0])
                elif len(timeValarr)==4:
                    timeVal=int(timeValarr[0])*60+int(timeValarr[2])
                dist=mins[1]
                if type=='bus stop':
                    allFieldsdict['bus_station_duration']=timeVal
                    allFieldsdict['bus_station_distance']=dist
                else:
                    allFieldsdict['subway_station_duration']=timeVal
                    allFieldsdict['subway_station_distance']=dist
        #redfin
        redfinResponse = apis.redFin(address)
        allFieldsdict.update(redfinResponse)
        #opendoor fields
        allFieldsdict['unique_id']=line['unique_id']
        allFieldsdict['scrape_source']="opendoor"
        allFieldsdict['created_at']=line['created_at']
        allFieldsdict['updated_at']=line['updated_at']
        allFieldsdict['resurface_reason']=line['resurface_reason']
        allFieldsdict['address']=address
        allFieldsdict['zip_code']=zip
        if 'lot_sq_ft' in line:
            allFieldsdict['lot_sqft']=line['lot_sq_ft']
        if 'bedrooms' in line:
            allFieldsdict['num_beds']=line['bedrooms']
        if 'bathrooms' in line:
            allFieldsdict['num_baths']=line['bathrooms']
        if 'year_built' in line:
            allFieldsdict['year_built']=line['year_built']
        if 'parking' in line:
            allFieldsdict['num_parking_spaces']=line['parking']
        if 'carport_spaces' in line:
            allFieldsdict['carport_spaces']=line['carport_spaces']
        if 'total_living_sq_ft' in line:
            allFieldsdict['living_sq_feet']=line['total_living_sq_ft']
        if 'interior_sq_ft' in line:
            allFieldsdict['interior_sq_ft']=line['interior_sq_ft']
        if 'id' in line:
            allFieldsdict['mlsNumber_or_id']=line['id']
        if 'city' in line:
            allFieldsdict['city']=line['city']
        if 'state' in line:
            allFieldsdict['state']=line['state']
        if 'county' in line:
            allFieldsdict['county']=str(line['county']) + " County"
        if 'roof' in line:
            allFieldsdict['roof']=line['roof_type']
        if 'solar_panels' in line:
            allFieldsdict['solar_panels']=line['solar_panels_ownership']
        if 'apn' in line:
            allFieldsdict['apn']=line['apn']
        if 'pool' in line:
            allFieldsdict['pool']=line['private_pool']
        if 'images_url' in line:
            allFieldsdict['images_url']=line['photo_url']
        if 'auction_details' in line:
            allFieldsdict['auction_end_time']=line['auction_details']['end_time']
            allFieldsdict['auction_start_time']=line['auction_details']['start_time']
        if 'dwelling_type' in line:
            allFieldsdict['dwelling_type']=line['dwelling_type']
        if 'estimated_current_yield' in line:
            allFieldsdict['estimated_current_yield']=line['estimated_current_yield']
        if 'flooring_type' in line:
            allFieldsdict['flooring_type']=line['flooring_type']
        if 'floors' in line:
            allFieldsdict['floors']=line['floors']
        if 'guidance_price' in line:
            if line['guidance_price'] != None:
                allFieldsdict['price']="$" + str(line['guidance_price'])
        if 'has_addition' in line:
            allFieldsdict['has_addition']=line['has_addition']
        if 'has_garage_conversion' in line:
            allFieldsdict['has_garage_conversion']=line['has_garage_conversion']
        if 'has_hoa_leasing_restrictions' in line:
            allFieldsdict['has_hoa_leasing_restrictions']=line['has_hoa_leasing_restrictions']
        if 'has_known_foundation_issues' in line:
            allFieldsdict['has_known_foundation_issues']=line['has_known_foundation_issues']
        if 'hoa_fee_usd' in line:
            if line['hoa_fee_usd'] != None:
                allFieldsdict['hoa_fee_usd']="$" + str(line['hoa_fee_usd'])
        if 'hoa_frequency' in line:
            allFieldsdict['hoa_frequency']=line['hoa_frequency']
        if 'hoa_rules' in line:
            allFieldsdict['hoa_rules']=line['hoa_rules']
        if 'kitchen_appliance_type' in line:
            allFieldsdict['kitchen_appliance_type']=line['kitchen_appliance_type']
        if 'kitchen_countertop' in line:
            allFieldsdict['kitchen_countertop']=line['kitchen_countertop']
        if 'opendoor_brokerage_pre_underwritten' in line:
            allFieldsdict['opendoor_brokerage_pre_underwritten']=line['opendoor_brokerage_pre_underwritten']
        if 'pre_uw_annual_hoa_fee_est_usd' in line:
            if line['pre_uw_annual_hoa_fee_est_usd'] != None:
                allFieldsdict['pre_uw_annual_hoa_fee_est_usd']="$" + str(line['pre_uw_annual_hoa_fee_est_usd'])
        if 'pre_uw_annual_ongoing_repair_expense_est_usd' in line:
            if line['pre_uw_annual_insurance_expense_est_usd'] != None:
                allFieldsdict['pre_uw_annual_ongoing_repair_expense_est_usd']="$" + str(line['pre_uw_annual_insurance_expense_est_usd'])
        if 'pre_uw_annual_ongoing_repair_expense_est_usd' in line:
            if line['pre_uw_annual_ongoing_repair_expense_est_usd'] != None:
                allFieldsdict['pre_uw_annual_ongoing_repair_expense_est_usd']="$" + str(line['pre_uw_annual_ongoing_repair_expense_est_usd'])
        if 'pre_uw_annual_property_mgmt_fee_est_usd' in line:
            if line['pre_uw_annual_property_mgmt_fee_est_usd'] != None:
                allFieldsdict['pre_uw_annual_property_mgmt_fee_est_usd']="$" + str(line['pre_uw_annual_property_mgmt_fee_est_usd'])
        if 'pre_uw_annual_property_tax_est_usd' in line:
            if line['pre_uw_annual_property_tax_est_usd'] != None:
                allFieldsdict['pre_uw_annual_property_tax_est_usd']="$" + str(line['pre_uw_annual_property_tax_est_usd'])
        if 'pre_uw_annual_rent_amount_high_est_usd' in line:
            if line['pre_uw_annual_rent_amount_high_est_usd'] != None:
                allFieldsdict['pre_uw_annual_rent_amount_high_est_usd']="$" + str(line['pre_uw_annual_rent_amount_high_est_usd'])
        if 'pre_uw_annual_rent_amount_low_est_usd' in line:
            if line['pre_uw_annual_rent_amount_low_est_usd'] != None:
                allFieldsdict['pre_uw_annual_rent_amount_low_est_usd']="$" + str(line['pre_uw_annual_rent_amount_low_est_usd'])
        if 'pre_uw_annual_rent_amount_mid_est_usd' in line:
            if line['pre_uw_annual_rent_amount_mid_est_usd'] != None:
                allFieldsdict['pre_uw_annual_rent_amount_mid_est_usd']="$" + str(line['pre_uw_annual_rent_amount_mid_est_usd'])
        if 'pre_uw_annual_vacancy_expense_est_usd' in line:
            if line['pre_uw_annual_vacancy_expense_est_usd'] != None:
                allFieldsdict['pre_uw_annual_vacancy_expense_est_usd']="$" + str(line['pre_uw_annual_vacancy_expense_est_usd'])
        if 'pre_uw_cap_rate_high_est' in line:
            allFieldsdict['pre_uw_cap_rate_high_est']=line['pre_uw_cap_rate_high_est']
        if 'pre_uw_cap_rate_low_est' in line:
            allFieldsdict['pre_uw_cap_rate_low_est']=line['pre_uw_cap_rate_low_est']
        if 'pre_uw_cap_rate_mid_est' in line:
            allFieldsdict['pre_uw_cap_rate_mid_est']=line['pre_uw_cap_rate_mid_est']
        if 'pre_uw_home_valuation_low_est_usd' in line:
            if line['pre_uw_home_valuation_low_est_usd'] != None:
                allFieldsdict['pre_uw_home_valuation_low_est_usd']="$" + str(line['pre_uw_home_valuation_low_est_usd'])
        if 'pre_uw_home_valuation_high_est_usd' in line:
            if line['pre_uw_home_valuation_high_est_usd'] != None:
                allFieldsdict['pre_uw_home_valuation_high_est_usd']="$" + str(line['pre_uw_home_valuation_high_est_usd'])
        if 'pre_uw_home_valuation_mid_est_usd' in line:
            if line['pre_uw_home_valuation_mid_est_usd'] != None:
                allFieldsdict['pre_uw_home_valuation_mid_est_usd']="$" + str(line['pre_uw_home_valuation_mid_est_usd'])
        if 'pre_uw_repair_amount_high_est_usd' in line:
            if line['pre_uw_repair_amount_high_est_usd'] != None:
                allFieldsdict['pre_uw_repair_amount_high_est_usd']="$" + str(line['pre_uw_repair_amount_high_est_usd'])
        if 'pre_uw_repair_amount_low_est_usd' in line:
            if line['pre_uw_repair_amount_low_est_usd'] != None:
                allFieldsdict['pre_uw_repair_amount_low_est_usd']="$" + str(line['pre_uw_repair_amount_low_est_usd'])
        if 'pre_uw_repair_amount_mid_est_usd' in line:
            if line['pre_uw_repair_amount_mid_est_usd'] != None:
                allFieldsdict['pre_uw_repair_amount_mid_est_usd']="$" + str(line['pre_uw_repair_amount_mid_est_usd'])
        if 'product_type' in line:
            allFieldsdict['product_type']=line['product_type']
        if 'sherlock_assessment' in line:
            allFieldsdict['sherlock_assessment']=line['sherlock_assessment']  
        #zillow
        zillowResp= apis.getZillowEstimate(address).split(',')
        zestimate=zillowResp[0]
        rentzestimate=zillowResp[1]
        lat=zillowResp[2]
        long=zillowResp[3]
        allFieldsdict['zestimate']=zestimate
        allFieldsdict['rentzestimate']=rentzestimate
        allFieldsdict['latitude']=lat
        allFieldsdict['longitude']=long
        if allFieldsdict["total_sqft"]=="" or int(allFieldsdict["total_sqft"])>1000:
            if line['resurface_reason'] == "price reduction" or line['resurface_reason'] == "price rising":
                cur.execute(
                    f"delete from ort.opendoor_properties where id='{line['id']}'"
                )
                connection.commit()
            #opendoor table
            dbvalues = list()
            dbvalues.append(allFieldsdict['mlsNumber_or_id'])
            dbvalues.append(allFieldsdict['price'])
            dbvalues.append(allFieldsdict['created_at'])
            cur.execute(
                f"insert into ort.opendoor_properties values ({', '.join(['%s'] * len(dbvalues))})",
                dbvalues,
            )
            connection.commit()
            #all table and sheet
            values=list(allFieldsdict.values())
            worksheet.append_row(values, value_input_option='USER_ENTERED')
            cur.execute(
                f"insert into ort.all_properties values ({', '.join(['%s'] * len(values))})",
                values,
            )
            connection.commit()
    connection.close()
insert_data()