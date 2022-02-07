from oauth2client.service_account import ServiceAccountCredentials
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium import webdriver
from datetime import date
import db_connect
import gspread
import shutil
import apis
import time
import csv
import os

def getResponse():
    data = list()
    datesdict={'unique_id':"",'created_at':"",'updated_at':"",'resurface_reason':""}
    initPath='C:\\projects\\ort'

    options = webdriver.ChromeOptions() 
    p = {'download.default_directory':initPath}
    options.add_experimental_option('prefs', p)
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Chrome(options=options)
    driver.get('https://login.stellarmls.com/idp/login')
    driver.execute_script("document.querySelectorAll('[type=hidden]')[0].removeAttribute('type')")
    driver.execute_script("document.querySelectorAll('[type=hidden]')[0].removeAttribute('type')")
    driver.execute_script("document.querySelector('#form_login div').removeAttribute('style')")

    user_selector = driver.find_element_by_css_selector('[name=username]')
    user_selector.send_keys('USERNAME')
    pass_selector = driver.find_element_by_css_selector('[name=password]')
    pass_selector.send_keys('PWD')
    
    driver.execute_script('document.querySelector("#form_login").submit();')

    time.sleep(5)
    driver.get('https://stellar.mlsmatrix.com/matrix/?f=')
    
    eplus_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, '[title*="[Brune, Jean] 1000+"]')))
    eplus_link.click()

    select_all = driver.find_element_by_css_selector('[title^="Check"]')
    select_all.click()

    export = driver.find_element_by_css_selector('.icon_export')
    export.click()

    dropdown = Select(driver.find_element_by_css_selector('#m_ddExport'))

    dropdown.select_by_value('ug67702')
    driver.find_element_by_css_selector('#m_tdExport').click()

    time.sleep(5)
    driver.quit()

    filename = max([initPath + "\\" + f for f in os.listdir(initPath)],key=os.path.getctime)
    shutil.move(filename,os.path.join(initPath,r"tampa.csv"))

    with open ("tampa.csv", "r", encoding="utf-8") as f:
        file = csv.DictReader(f, delimiter=",")

        for row in file:
            temp = {}
            temp.update(row)
            temp.update(datesdict)
            data.append(temp)
    f.close()
    os.remove(initPath + '\\tampa.csv')
    return data

def newRecControl():
    properties = getResponse()
    connection = db_connect.get_connection()
    cur = connection.cursor()
    cur.execute("SELECT mls_number,cdom,current_price,created_at,status from ort.tampamls_properties")
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

    for row in rows:
        temp=0
        dom=str(row[1])
        if dom !="":  
            if int(dom)==45 or int(dom)==90:
                temp=1
        for prop in properties:
            proID = str(prop['ML Number'])
            newPrice=float(str(prop['Current Price']).replace("$","").replace(",",""))
            if proID == str(row[0]):
                createdDate=row[3]
                oldPrice = float(str(row[2]).replace("$","").replace(",",""))
                if createdDate == today: 
                    if newPrice < oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price reduction"
                        refuses.append(prop)
                    elif newPrice > oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price increase"
                        refuses.append(prop)           
                    elif str(row[4]) != str(prop['Status']):
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="status change"
                        refuses.append(prop)                
                    del properties[properties.index(prop)]
                else:
                    if newPrice < oldPrice:
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="price reduction"
                        refuses.append(prop)
                        del properties[properties.index(prop)]
                    elif newPrice > oldPrice:
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
                    elif str(row[4]) != str(prop['Status']):
                        prop['created_at']=createdDate
                        prop['updated_at']=today
                        prop['resurface_reason']="status change"
                        refuses.append(prop)                       
                        del properties[properties.index(prop)]
    for line in properties:
        line['created_at']=today                  
    for line in refuses:
        properties.append(line)
    for line in properties:
        line['unique_id']=unique_id
        unique_id = unique_id+1
    connection.commit()
    connection.close()
    return properties

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

    newRecList = newRecControl()  
    stationTypes=['bus stop','subway stop']
    for line in newRecList:
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
        address = line['Address']
        zip=str(line['Zip'])
        location = address + ", " + zip
        #redfin
        redfinResponse = apis.redFin(address)
        allFieldsdict.update(redfinResponse)
        #google
        timeVal=str()
        dist=str()
        city=str()
        state=str()
        country=str()
        once=1
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
                if once==1:
                    city = mins[2]
                    state = mins[3]
                    country = mins[4]
                    once = 2           
                if type=='bus stop':
                    allFieldsdict['bus_station_duration']=timeVal
                    allFieldsdict['bus_station_distance']=dist
                else:
                    allFieldsdict['subway_station_duration']=timeVal
                    allFieldsdict['subway_station_distance']=dist
        #harsmls fields
        allFieldsdict['unique_id']=line['unique_id']
        allFieldsdict['scrape_source']="tampamls"
        allFieldsdict['created_at']=line['created_at']
        allFieldsdict['updated_at']=line['updated_at']
        allFieldsdict['resurface_reason']=line['resurface_reason']
        allFieldsdict['mlsNumber_or_id']=line['ML Number']
        allFieldsdict['address']=address
        allFieldsdict['city']=city
        allFieldsdict['state']=state
        allFieldsdict['county']=country
        allFieldsdict['zip_code']=zip
        allFieldsdict['price']=line['Current Price']
        allFieldsdict['status']=line['Status']
        allFieldsdict['total_days_on_market']=line['CDOM'] 
        allFieldsdict['days_on_market']=line['ADOM'] 
        allFieldsdict['closed_date']=line['Close Date'] 
        allFieldsdict['current_price_per_sqft']=line['SP / SqFt'] 
        allFieldsdict['num_beds']=line['Beds']  
        allFieldsdict['numberof_full_baths']=line['Full Baths']  
        allFieldsdict['numberof_half_baths']=line['Half Baths']  
        allFieldsdict['num_baths']=line['Bathrooms Total']   
        allFieldsdict['year_built']=line['Year Built'] 
        allFieldsdict['numof_garage_spaces']=line['Garage Spaces'] 
        allFieldsdict['lot_size']=line['Lot Size Acres']  
        allFieldsdict['floors']=line['Total # of Floors']
        allFieldsdict['tax_amount']=line['Monthly HOA Amount']
        allFieldsdict['hoa_fee_usd']=line['HOA Fee']
        allFieldsdict['association_yes_or_no']=line['Association Approval Required Y/N']
        allFieldsdict['maint_fee_amt']=line['Monthly Maint Fee (in Addn to HOA)']
        allFieldsdict['taxes_due']=line['Tax']  
        allFieldsdict['hoa_fee_requirement']=line['HOA Fee Requirement']  
        allFieldsdict['heated_area']=line['Heated Area']     
        if allFieldsdict['elementaryS_name']=="":
            allFieldsdict['elementaryS_name']=line['Elementary School']
        if allFieldsdict['highS_name']=="":
            allFieldsdict['highS_name']=line['High School']
        if allFieldsdict['subdivision_name']=="":
            allFieldsdict['subdivision_name']=line['Legal Subdivision Name'] 
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
        if line['resurface_reason'] != '':
            cur.execute(
                f"delete from ort.tampamls_properties where mls_number='{line['ML Number']}'"
            )
            connection.commit()
        #harmls table
        dbvalues = list()
        dbvalues.append(allFieldsdict['mlsNumber_or_id'])
        dbvalues.append(allFieldsdict['price'])
        dbvalues.append(allFieldsdict['total_days_on_market'])
        dbvalues.append(allFieldsdict['created_at'])
        dbvalues.append(allFieldsdict['status'])
        cur.execute(
            f"insert into ort.tampamls_properties values ({', '.join(['%s'] * len(dbvalues))})",
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