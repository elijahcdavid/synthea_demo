import pandas as pd
from tqdm import tqdm
from geopy.geocoders import Nominatim
from geopy.location import Location

tqdm.pandas()

def get_country_from_lat_long(lat, long):
    geolocator = Nominatim(user_agent='geo_app')
    coordinates = f'{lat}, {long}'

    try:
        location: Location = geolocator.reverse(coordinates, exactly_one=True, language='en')
        return location.raw['address']['county']
    except Exception as e:
        return None

def safe_extract(resource, keys):
    try:
        for key in keys:
            resource = resource[key]
        return resource
    except (KeyError, IndexError, TypeError):
        return None

def extract_patient_data(resource):
    # Extract latitude and longitude
    latitude = safe_extract(resource, ['address', 0, 'extension', 0, 'extension', 0, 'valueDecimal'])
    longitude = safe_extract(resource, ['address', 0, 'extension', 0, 'extension', 1, 'valueDecimal'])

    new_record = {
        'id': resource['id'],
        'birth_date': safe_extract(resource, ['birthDate']),
        'deceased_date': safe_extract(resource, ['deceasedDateTime']),
        'first_name': safe_extract(resource, ['name', 0, 'given', 0]),
        'middle_name': safe_extract(resource, ['name', 0, 'given', 1]),
        'last_name': safe_extract(resource, ['name', 0, 'family']),
        'marital': safe_extract(resource, ['maritalStatus', 'coding', 0, 'code']),
        'race': safe_extract(resource, ['extension', 0, 'extension', 0, 'valueCoding', 'display']),
        'ethnicity': safe_extract(resource, ['extension', 1, 'extension', 0, 'valueCoding', 'display']),
        'gender': safe_extract(resource, ['gender']),
        'birth_place_city': safe_extract(resource, ['extension', 4, 'valueAddress', 'city']),
        'birth_place_state': safe_extract(resource, ['extension', 4, 'valueAddress', 'state']),
        'birth_place_country': safe_extract(resource, ['extension', 4, 'valueAddress', 'country']),
        'street': safe_extract(resource, ['address', 0, 'line', 0]),
        'city': safe_extract(resource, ['address', 0, 'city']),
        'state': safe_extract(resource, ['address', 0, 'state']),
        'county': get_country_from_lat_long(latitude, longitude),
        'country': safe_extract(resource, ['address', 0, 'country']),
        'zip': safe_extract(resource, ['address', 0, 'postalCode']),
        'latitude': latitude,
        'longitude': longitude
    }
    return new_record

def extract_encounter_data(resource):
    new_record = {
        'id': resource['id'],
        'start': safe_extract(resource, ['period', 'start']),
        'finish': safe_extract(resource, ['period', 'end']),
        'patient': safe_extract(resource, ['subject', 'reference'] or '').replace('urn:uuid:', ''),
        'provider': safe_extract(resource, ['serviceProvider', 'display']),
        'encounter_class': safe_extract(resource, ['class', 'code']),
        'code': ', '.join(str(safe_extract(code, ['code'])) for code in safe_extract(resource, ['type', 0, 'coding']) or []),
        'description': ', '.join(str(safe_extract(code, ['display'])) for code in safe_extract(resource, ['type', 0, 'coding'] or [])),
    }
    return new_record

def extract_condition_data(resource):
    new_record = {
        'id': resource['id'],
        'onset_datetime': safe_extract(resource, ['onsetDateTime']),
        'recorded_date': safe_extract(resource, ['recordedDate']),
        'patient': safe_extract(resource, ['subject', 'reference'] or '').replace('urn:uuid:', ''),
        'encounter': safe_extract(resource, ['encounter', 'reference'] or '').replace('urn:uuid:', ''),
        'code': ', '.join(str(safe_extract(code, ['code'])) for code in safe_extract(resource, ['code', 'coding']) or []),
        'description': ', '.join(str(safe_extract(code, ['display'])) for code in safe_extract(resource, ['code', 'coding']) or []),
    }
    return new_record

def extract_diagnostic_data(resource):
    new_record = {
        'id': resource['id'],
        'patient': safe_extract(resource, ['subject', 'reference'] or '').replace('urn:uuid:', ''),
        'encounter': safe_extract(resource, ['encounter', 'reference'] or '').replace('urn:uuid:', ''),
        'code': ', '.join(str(safe_extract(code, ['code'])) for code in safe_extract(resource, ['code', 'coding']) or []),
        'description': ', '.join(str(safe_extract(code, ['display'])) for code in safe_extract(resource, ['code', 'coding']) or []),
    }
    return new_record

def extract_claim_data(resource):
    new_record = {
        'id': resource['id'],
        'created': safe_extract(resource, ['created']),
        'patient': safe_extract(resource, ['patient', 'reference'] or '').replace('urn:uuid:', ''),
        'provider': safe_extract(resource, ['provider', 'display']),
        'diagnostic_report': safe_extract(resource, ['created']),
        'coverage': safe_extract(resource, ['insurance', 0, 'coverage', 'display']),
        'item_code': ', '.join(str(safe_extract(item, ['productOrService', 'coding', 0, 'code'])) for item in safe_extract(resource, ['item']) or []),
        'item': ', '.join(str(safe_extract(item, ['productOrService', 'coding', 0, 'display'])) for item in safe_extract(resource, ['item']) or []),
        'total': safe_extract(resource, ['total', 'value'])
    }
    return new_record

def load_data_to_df(path):
    patient_df, encounter_df, condition_df, diagnostic_df, claim_df = (pd.DataFrame() for _ in range(5))
    patient_records, encounter_records, condition_records, diagnostic_records, claim_records = ([] for _ in range(5))
    # iterate through the Synthea files

    for file in tqdm(path.glob('*.json'), desc="Processing data"):
        try:
            json_data = pd.read_json(file)

            for entry in json_data['entry']:
                resource_type = entry['resource']['resourceType']

                if resource_type == 'Patient':
                    patient_records.append(extract_patient_data(entry['resource']))
                elif resource_type == 'Encounter':
                    encounter_records.append(extract_encounter_data(entry['resource']))
                elif resource_type == 'Condition':
                    condition_records.append(extract_condition_data(entry['resource']))
                elif resource_type == 'DiagnosticReport':
                    diagnostic_records.append(extract_diagnostic_data(entry['resource']))
                elif resource_type == 'Claim':
                    claim_records.append(extract_claim_data(entry['resource']))
                else:
                    continue
        except Exception as e:
            print(f"Error processing file {file}: {type(e).__name__}: {e}")

    patient_df = pd.DataFrame(patient_records)
    encounter_df = pd.DataFrame(encounter_records)
    condition_df = pd.DataFrame(condition_records)
    diagnostic_df = pd.DataFrame(diagnostic_records)
    claim_df = pd.DataFrame(claim_records)

    return {'patient_df': patient_df, 
            'encounter_df': encounter_df, 
            'condition_df': condition_df, 
            'diagnostic_df': diagnostic_df, 
            'claim_df': claim_df
    } 