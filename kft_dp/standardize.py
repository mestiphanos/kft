# check if a phone number is all digits
# _> if so check length, if 0 found at the beginning remove it
from datetime import datetime

def standardize_phone_number(phone_number):
    phone_number = ('').join([digit for digit in str(phone_number) if digit.isdigit()])
    if len(phone_number) > 9:
        if phone_number.startswith('+251'):
            phone_number = phone_number[4:]
        elif phone_number.startswith('251'):
            phone_number = phone_number[3:]
        elif phone_number.startswith('09'):
            phone_number = phone_number[1:] 
        
    if len(phone_number) > 9:
        if phone_number.startswith('011'):
            print(phone_number,"Seems like phone number for an office/home")
        else:
            print(phone_number,"phone numbers to be investigated")
     
    return phone_number
    
def standardize_email(email,common_email_services=['yahoo.com','gmail.com','shega.com','kifiya.et']):
    email_domain_names = ['.com','.et']
    email_symbols = ['@']
    # for domain_name in email_domain_names:
    if not isinstance(email,str):
        email = str(email)
        
        if email != 'nan':
            if not email.find(email_symbols[0]):
                print("couldn't find @")
            else:
                email_id = email.split('@')[0]
                email_service = email.split('@')[1].lower()
                if email_service not in common_email_services:
                    try:
                        service_start_idx = [common_service[0] for common_service in common_email_services].index(email_service[0])
                        email_service = common_email_services[service_start_idx]
                    except:
                        print(email,"contains unknown email service")
            email = ('@').join([email_id,email_service])
            if not email.endswith('.com') and not email.endswith('.et'):
                print(email," doesnot end with one of the domain names ",email_domain_names)
    return email


def standardize_year(year):
    year = str(year)
    try:
        year = datetime.strptime(year, '%Y').year
    except:
        if len(year) != 4:
            year = ('').join([digit for digit in str(year) if digit.isdigit()])               
            if year:
                if len(year) == 5 and year.endswith("0"):
                    year = year[:-1]
                try:
                    year = datetime.strptime(year, '%Y').year
                except:
                    print(year,"After removing non-digit values still couldn't be converted to year")
                    
        else:
            year = year.replace('ዐ','0')
            try:
                year = datetime.strptime(year, '%Y').year
            except:
                print(year,"Length is 4 but couldn't be converted to year")
    return year
    
