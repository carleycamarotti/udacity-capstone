import pandas as pd

def read_lines():
    """
    Read in 
    
    """
    codes = 'I94_SAS_Labels_Descriptions.SAS'
    with open(codes) as f:
        lines = f.readlines()
        
    lines = [line.replace('"','').replace('\n','').replace("'",'').replace('\t','') for line in lines]
    return lines

def country_dictionary(lines):
    origin_country = lines[9:298]
    oc_codes = [x.split('=')[0].replace(' ', '') for x in origin_country]
    oc_countries = [' '.join(x.split('=')[-1].split(' ')[2:]) for x in origin_country]
    oc = dict(zip(oc_codes, oc_countries))
    return oc

def port_df(lines):
    ports = lines[302:961]
    p_codes = [x.split('=')[0].replace(' ', '') for x in ports]
    p_city = [x.split('=')[-1].split(',')[0] for x in ports]
    p_state = [x.split('=')[-1].split(',')[-1].replace(' ', '') for x in ports]
    
    port_df = pd.DataFrame({
        'port_code': p_codes,
        'port_city': p_city,
        'port_state': p_state
    })
    return port_df
    
    
lines = read_lines()
country_codes = country_dictionary(lines)
port_codes = port_df(lines)
