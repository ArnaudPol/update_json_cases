#===
#===
# LOST DATA:    'diagnoses' > 'additionalDrugs'
#               'diagnoses' > 'additionalManagements'
#               'diagnoses' > 'customDrugs' (no data)
#===
#===
#=== IMPORT
import os
from pathlib import Path
import fnmatch
import json
import datetime
import uuid
import zipfile



#=== GLOBAL
DATE_STR_SIZE = 19
FACT_SEC_TO_MIL = 1000

DEFAULT_JSON_TEXT = None
DEFAULT_JSON_VERSION = 99
DEFAULT_ADVANCEMENT_VALUES = 0
DEFAULT_BIRTH_DATE_ESTIMATED = False
DEFAULT_BIRTH_DATE_ESTIMATED_TYPE = 'month'
DEFAULT_ACTIVITIES = []
DEFAULT_FORMULATION_ID = 0
DEFAULT_ADDITIONAL_DRUGS = {}
DEFAULT_CUSTOM_DRUGS = {}
DEFAULT_DIAGNOSIS_EXCLUDED = []

MSG_DIRECTORY = "Checking directory..."
MSG_DIRECTORY_FAIL = "Directory 'cases' does not exist. Before running the program, please create a new 'cases' directory containing .zip or .json files that you want to update."

MSG_FILES = "Checking files..."
MSG_FILE_FOUND_FILE = "file"
MSG_FILE_FOUND_FILES = "files"
MSG_FILES_FOUND = "Found {n_zip} zip {_zip_file} and {n_json} json {_json_file}."
MSG_FILES_FOUND_FILE = "file has"
MSG_FILES_FOUND_FILES = "files have"
MSG_FILES_INFO = "Out of {n_total} {_file}, \n\t {n_processed} \t {_processed_file} been processed, \n\t {n_ignored} \t {_ignored_file} been ignored and \n\t {n_failed} \t {_failed_file} failed during processing."

MSG_PROCESS = "Processing cases..."
MSG_PROCESS_DIR_FAIL = "Cannot create new directory 'new_cases'."

MSG_CONTINUE = "Press enter to start processing cases."
MSG_SUCCESSFUL = "Done!"
MSG_ABORT = "Program failed."

MSG_ERROR_ASSERTION = "Assertion error in file {file}: "
MSG_ERROR_CONTACT = "Please contact an administrator."
MSG_ERROR_FILE = "Error in file {file}: "
MSG_ERROR_KEY = "Key {key} does not exist."

STUDY_DYN_RW = "Dynamic Rwanda"
STUDY_DYN_TZ = "Dynamic Tanzania"
STUDY_TIMCI_SN = "TIMCI Senegal"
STUDY_TIMCI_TZ = "TIMCI Tanzania"
STUDY_TO_ID = {STUDY_DYN_TZ     : "1",
               STUDY_TIMCI_TZ   : "2",
               STUDY_DYN_RW     : "3",
               STUDY_TIMCI_SN   : "4"}

DATA_NOTES_TRANSLATIONS_FILE_PATH = "data/nodes_translations.json"
DATA_FILE_PATH = 'data/data_{version_id}.json'



#=== FUNCTIONS
# Returns the server parameters
def loadParameters():
    with open(DATA_NOTES_TRANSLATIONS_FILE_PATH, encoding='utf-8') as json_file:
            return json.load(json_file)

# Returns the data version parameters
def loadData(version_id):
    with open(DATA_FILE_PATH.format(version_id=version_id), encoding='utf-8') as json_file:
            return json.load(json_file)

# Checks if 'cases' directory exists.
def checkDirectory(path_to_cases):
    print(MSG_DIRECTORY)

    if(path_to_cases.exists() and path_to_cases.is_dir()):
        print(MSG_SUCCESSFUL)
    else:
        print(MSG_DIRECTORY_FAIL)
        print(MSG_ABORT)
        exit()

# Gives files informations.
def checkFiles(path_to_cases):
    print(MSG_FILES)

    n_zip   = len(fnmatch.filter(os.listdir(path_to_cases), '*.zip'))
    n_json  = len(fnmatch.filter(os.listdir(path_to_cases), '*.json'))

    _zip_file   = MSG_FILE_FOUND_FILE if n_zip == 1 else MSG_FILE_FOUND_FILES
    _json_file  = MSG_FILE_FOUND_FILE if n_json == 1 else MSG_FILE_FOUND_FILES

    print(MSG_FILES_FOUND.format(n_zip = n_zip, n_json = n_json, _zip_file=_zip_file, _json_file=_json_file))
    print(MSG_SUCCESSFUL)

# If true, the programm will not process the file.
def jsonIsUpToDate(data):
    return 'advancement' in data

# Create the new directory to add new jsons.
def createNewDirectory():
    path_to_new_cases = Path().resolve().joinpath("new_cases")
    try:
        if(not path_to_new_cases.exists()):
            os.mkdir(path_to_new_cases)
    except OSError:
        print(MSG_PROCESS_DIR_FAIL)
        print(MSG_ABORT)
        exit()

# Returns the id of the case study
def getCaseStudy(data):
    return data['patient']['study_id']

def getVersionId(data):
    return data['version_id']

def getParamName(data):
    return str(getCaseStudy(data)) + " " + str(getVersionId(data))

# Get zip files and update json cases
def processZipCases():
    n_total = 0
    n_processed = 0
    n_failed = 0
    n_ignored = 0

    zip_file_names = fnmatch.filter(os.listdir(path_to_cases), '*.zip')
    for zip_file_name in zip_file_names:
        # Read zip archive
        with zipfile.ZipFile("cases/" + zip_file_name, 'r') as read_archive:
            # Create a new zip archive
            with zipfile.ZipFile("new_cases/" + zip_file_name, 'w') as write_archive:
                file_names = read_archive.namelist()
                json_file_names = [elem for elem in file_names if '.json' in elem]
                # Extract data from json files
                for json_file_name in json_file_names:
                    n_total += 1
                    json_file = read_archive.read(json_file_name)
                    data = json.loads(json_file)
                    version_json = loadData(getVersionId(data))

                    # Ignore files that already have correct structure
                    if(jsonIsUpToDate(data)):
                        n_ignored += 1
                        continue

                    try:
                        new_data = processCase(data, param_data, version_json)
                        n_processed += 1
                        # Write file
                        new_data = json.dumps(new_data, separators=(',', ':'))
                        write_archive.writestr(json_file_name, new_data)
                    #except KeyError as e:
                    #    print(MSG_ERROR_FILE.format(file = json_file_name) + MSG_ERROR_KEY.format(key = str(e)))
                    #    n_failed += 1
                    except AssertionError as e:
                        print(MSG_ERROR_ASSERTION.format(file = json_file_name) + str(e))
                        print(MSG_ERROR_CONTACT)
                        n_failed += 1

    return n_total, n_processed, n_failed, n_ignored

# Get json cases and update them
def processJsonCases():
    n_total     = 0
    n_processed = 0
    n_failed    = 0
    n_ignored   = 0

    json_file_names = fnmatch.filter(os.listdir(path_to_cases), '*.json')
    for json_file_name in json_file_names:
        n_total += 1
        print(json_file_name)

        with open("cases/" + json_file_name, encoding='utf-8') as json_file:
            data = json.load(json_file)
            version_json = loadData(getVersionId(data))

            # Ignore files that already have correct structure
            if(jsonIsUpToDate(data)):
                n_ignored += 1
                continue
            
            try:
                new_data = processCase(data, param_data, version_json)
                n_processed += 1
                # Write file
                with open('new_cases/' + json_file_name, 'w') as json_file:
                    json.dump(new_data, json_file, separators=(',', ':'))
            #except KeyError as e:
            #    print(MSG_ERROR_FILE.format(file = json_file_name) + MSG_ERROR_KEY.format(key = str(e)))
            #    n_failed += 1
            except AssertionError as e:
                print(MSG_ERROR_ASSERTION.format(file = json_file_name) + str(e))
                print(MSG_ERROR_CONTACT)
                n_failed += 1

    return n_total, n_processed, n_failed, n_ignored

# Processes all the cases
# Returns the total number of files, processed files, failed files and ignored files
def processCases(path_to_cases, param_data):
    print(MSG_PROCESS)
    n_total     = 0
    n_processed = 0
    n_failed    = 0
    n_ignored   = 0

    createNewDirectory()

    n_zip_total, n_zip_processed, n_zip_failed, n_zip_ignored       = processZipCases()
    n_json_total, n_json_processed, n_json_failed, n_json_ignored   = processJsonCases()

    n_total     = n_zip_total + n_json_total
    n_processed = n_zip_processed + n_json_processed
    n_failed    = n_zip_failed + n_json_failed
    n_ignored   = n_zip_ignored + n_json_ignored

    return n_total, n_processed, n_failed, n_ignored

def updateId(data):
    return data['id']

def updateActivities(data, param_data):
    return DEFAULT_ACTIVITIES

def updateDiagnosisProposed(diagnosis):
    return [int(n) for n in diagnosis['proposed'].keys()]

def updateDiagnosisId(diagnosis):
    return diagnosis['id']

def updateDiagnosisManagements(diagnosis):
    return [int(n) for n in diagnosis['managements'].keys()]

def updateDiagnosisDrugsProposed(drugs):
    return [int(n) for n in drugs.keys()]

def updateDrugsId(drug):
    return drug['id']

def updateDrugsBoolFields(drug, version_json):
    drug_id = updateDrugsId(drug)
    drug_json = version_json['medal_r_json']['health_cares'][str(drug_id)]

    new_is_anti_malarial    = drug_json['is_anti_malarial']
    new_is_antibiotic       = drug_json['is_antibiotic']
    
    return new_is_anti_malarial, new_is_antibiotic

def updateDrugsFormulationId(data, drug, param_data):
    drugs_formulation_ids = param_data[getParamName(data)]['drugs_formulation_ids']
    keys = drugs_formulation_ids.keys()
    if str(updateDrugsId(drug)) in keys:
        formulation_ids = param_data[getParamName(data)]['drugs_formulation_ids'][str(updateDrugsId(drug))]
        return formulation_ids[0]
    else:
        return DEFAULT_FORMULATION_ID

def updateDiagnosisAgreedDrugsInstance(data, drug, param_data, version_json):
    new_agreed = {}
    new_agreed['id']                                            = updateDrugsId(drug)
    new_agreed['is_anti_malarial'], new_agreed['is_antibiotic'] = updateDrugsBoolFields(drug, version_json)
    new_agreed['formulation_id']                                = updateDrugsFormulationId(data, drug, param_data)

    return new_agreed

def updateDiagnosisDrugsAgreedRefused(data, drugs, param_data, version_json):
    proposed_drugs = drugs
    new_agreed = {}
    new_refused = []
    for key in proposed_drugs.keys():
        drug = proposed_drugs[key]
        if('agreed' not in drug):
            continue

        is_agreed = drug['agreed']
        if(is_agreed):
            new_agreed[key] = updateDiagnosisAgreedDrugsInstance(data, drug, param_data, version_json)
        else:
            new_refused.append(int(key))
    
    return new_agreed, new_refused

def updateDiagnosisDrugsAdditional(data, drugs):
    additional_drugs = data['diagnoses']['additionalDrugs']
    for key in additional_drugs.keys():
        additional_drug = additional_drugs[key]
        if 'diagnoses' not in additional_drug:
            return DEFAULT_ADDITIONAL_DRUGS

        diagnoses = additional_drug['diagnoses']
        if not (len(diagnoses) == 1 and diagnoses[0] is None):
            raise AssertionError("Additional drug does have a link to a diagnosis!")

    return DEFAULT_ADDITIONAL_DRUGS

def updateDiagnosisAgreedDrugs(data, agreed, param_data, version_json):
    drugs = agreed['drugs']
    new_drugs = {}
    new_drugs['proposed']                       = updateDiagnosisDrugsProposed(drugs)
    new_drugs['agreed'], new_drugs['refused']   = updateDiagnosisDrugsAgreedRefused(data, drugs, param_data, version_json)
    new_drugs['additional']                     = updateDiagnosisDrugsAdditional(data, drugs)

    return new_drugs

def updateDiagnosisAgreedInstance(data, agreed, param_data, version_json):
    new_agreed_instance = {}
    new_agreed_instance['id']           = updateDiagnosisId(agreed)
    new_agreed_instance['managements']  = updateDiagnosisManagements(agreed)
    new_agreed_instance['drugs']        = updateDiagnosisAgreedDrugs(data, agreed, param_data, version_json)

    return new_agreed_instance

def updateDiagnosisAgreedRefused(data, diagnosis, param_data, version_json):
    proposed_diagnoses = diagnosis['proposed']
    new_agreed = {}
    new_refused = []
    for key in proposed_diagnoses.keys():
        diag = proposed_diagnoses[key]
        is_agreed = diag['agreed']
        if(is_agreed):
            new_agreed[key] = updateDiagnosisAgreedInstance(data, diag, param_data, version_json)
        else:
            new_refused.append(int(key))
    
    return new_agreed, new_refused

def updateDiagnosisExcluded():
    return DEFAULT_DIAGNOSIS_EXCLUDED

def updateDiagnosisAdditionalManagements(additional):
    return [int(n) for n in additional['managements'].keys()]

def updateDiagnosisAdditionalDrugsCustom(data, drugs):
    custom_drugs = data['diagnoses']['customDrugs']
    for key in custom_drugs:
        custom_drug = custom_drugs[str(key)]
        if 'diagnoses' not in custom_drug:
            return DEFAULT_CUSTOM_DRUGS

        diagnoses = custom_drug['diagnoses']
        if not (len(diagnoses) == 1 and diagnoses[0] is None):
            raise AssertionError("Custom drug does have a link to a diagnosis!")

    return DEFAULT_CUSTOM_DRUGS

def updateDiagnosisAdditionalDrugs(data, additional, param_data, version_json):
    drugs = additional['drugs']
    new_drugs = {}
    new_drugs['proposed']                       = updateDiagnosisDrugsProposed(drugs)
    new_drugs['agreed'], new_drugs['refused']   = updateDiagnosisDrugsAgreedRefused(data, drugs, param_data, version_json)
    new_drugs['additional']                     = updateDiagnosisDrugsAdditional(data, drugs)
    new_drugs['custom']                         = updateDiagnosisAdditionalDrugsCustom(data, additional)

    return new_drugs

def updateDiagnosisAdditionalInstance(data, additional, param_data, version_json):
    new_additional_instance = {}
    new_additional_instance['id']           = updateDiagnosisId(additional)
    new_additional_instance['managements']  = updateDiagnosisManagements(additional)
    new_additional_instance['drugs']        = updateDiagnosisAdditionalDrugs(data, additional, param_data, version_json)

    return new_additional_instance

def updateDiagnosisAdditional(data, diagnosis, param_data, version_json):
    additionals = diagnosis['additional']
    new_additional = {}
    for key in additionals.keys():
        diag = additionals[key]
        new_additional[key] = updateDiagnosisAdditionalInstance(data, diag, param_data, version_json)

    return new_additional

def updateDiagnosisCustomName(custom):
    return custom['label']

def updateDiagnosisCustomDrugName(drug):
    return drug['label']

def updateDiagnosisCustomDrugDuration(drug):
    return drug['duration']

def updateDiagnosisCustomDrugsInstance(data, drug, drug_uuid):
    new_drug_instance = {}
    if(isinstance(drug, str)): # 39
        new_drug_instance['id']   = drug_uuid
        new_drug_instance['name'] = updateDiagnosisCustomDrugName(drug)
        new_drug_instance['is_anti_malarial'], new_drug_instance['is_antibiotic']  = None, None
    else: # 41
        new_drug_instance['id']         = drug_uuid
        new_drug_instance['is_anti_malarial'], new_drug_instance['is_antibiotic']  = updateDrugsBoolFields(data, drug)
        new_drug_instance['name']       = updateDiagnosisCustomDrugName(drug)
        new_drug_instance['duration']   = updateDiagnosisCustomDrugDuration(drug)

    return new_drug_instance

def updateDiagnosisCutomDrugs(data, custom):
    drugs = custom['drugs']
    new_drugs = {}
    for drug in drugs:
        drug_uuid = str(uuid.uuid1())
        new_drugs[drug_uuid] = updateDiagnosisCustomDrugsInstance(data, drug, drug_uuid)
    
    return new_drugs

def updateDiagnosisCustomInstance(data, custom, custom_uuid):
    new_custom_instance = {}
    new_custom_instance['id']       = custom_uuid
    new_custom_instance['name']     = updateDiagnosisCustomName(custom)
    new_custom_instance['drugs']    = updateDiagnosisCutomDrugs(data, custom)

    return new_custom_instance

def updateDiagnosisCustom(data, diagnosis):
    customs = diagnosis['custom']
    new_custom = {}
    for custom in customs:
        custom_uuid = str(uuid.uuid1())
        new_custom[custom_uuid] = updateDiagnosisCustomInstance(data, custom, custom_uuid)

    return new_custom

def updateManagementsFieldsInDiagnosis(new_diagnosis, diagnosis_type):
    for key in new_diagnosis[diagnosis_type].keys():
        diagnosis = new_diagnosis[diagnosis_type][key]
        managements = diagnosis['managements']

        n_management = 0
        for management in managements:
            new_diagnosis[diagnosis_type][key][str(n_management)] = management
            n_management += 1

    return new_diagnosis

def updateManagementsFields(new_diagnosis):
    new_diagnosis = updateManagementsFieldsInDiagnosis(new_diagnosis, 'agreed')
    new_diagnosis = updateManagementsFieldsInDiagnosis(new_diagnosis, 'additional')

    return new_diagnosis


def updateDiagnosis(data, param_data, version_json):
    diagnosis = data['diagnoses']
    new_diagnosis = {}

    new_diagnosis['proposed']                           = updateDiagnosisProposed(diagnosis)
    new_diagnosis['excluded']                           = updateDiagnosisExcluded()
    new_diagnosis['additional']                         = updateDiagnosisAdditional(data, diagnosis, param_data, version_json)
    new_diagnosis['agreed'], new_diagnosis['refused']   = updateDiagnosisAgreedRefused(data, diagnosis, param_data, version_json)
    new_diagnosis['custom']                             = updateDiagnosisCustom(data, diagnosis)

    # management list is also displayed as fields... which is weird
    if(getVersionId(data) == 39):
        new_diagnosis = updateManagementsFields(new_diagnosis)

    return new_diagnosis

def updateComment(data):
    return data['comment']

def updateConsent(data):
    return data['consent']

def getNodeType(node):
    return node['type']

def updateNodeId(node):
    return node['id']

def updateQuestionNodeAnswer(node):
    if('answer' not in node): # 39
        return None
    return node['answer']

def updateQuestionNodeValue(node):
    return node['value']

def updateQuestionNodeRoundedValue(node):
    return node['roundedValue']

def updateQuestionNodeValidationMessage(node):
    return node['validationMessage']

def updateQuestionNodeValidationType(node):
    return node['validationType']

def updateQuestionNodeUnavailableValue(node):
    return node['unavailableValue']

def updateQuestionSequenceNodeAnswer(node):
    return node['answer']

def updateQuestionNode(node):
    new_node = {}
    new_node['id']                  = updateNodeId(node)
    new_node['answer']              = updateQuestionNodeAnswer(node)
    new_node['value']               = updateQuestionNodeValue(node)
    if('roundedValue' in node.keys()):
        new_node['roundedValue']    = updateQuestionNodeRoundedValue(node)
    new_node['validationMessage']   = updateQuestionNodeValidationMessage(node)
    new_node['validationType']      = updateQuestionNodeValidationType(node)
    new_node['unavailableValue']    = updateQuestionNodeUnavailableValue(node)

    return new_node

def updateQuestionsSequenceNode(node):
    new_node = {}
    new_node['id']      = updateNodeId(node)
    new_node['answer']  = updateQuestionSequenceNodeAnswer(node)

    return new_node

def updateNodes(data):
    nodes = data['nodes']
    new_nodes = {}
    # Node types are ['Question', 'HealthCare', 'QuestionsSequence', 'FinalDiagnostic']
    for key in nodes.keys():
        node = nodes[key]
        nodeType = getNodeType(node)

        # HealthCare and FinalDiagnostic nodes never appear in new version.
        if(nodeType == 'Question'): 
            new_nodes[key] = updateQuestionNode(node)
        elif(nodeType == 'QuestionsSequence'):
            new_nodes[key] = updateQuestionsSequenceNode(node)
        elif(nodeType != 'HealthCare' and nodeType != 'FinalDiagnostic'):
            raise AssertionError(f"Unknown node type {nodeType}!")

    return new_nodes

def updateJson(data):
    return DEFAULT_JSON_TEXT

def updateJsonVersion():
    return DEFAULT_JSON_VERSION

def updateAdvancement():
    return {'stage' : DEFAULT_ADVANCEMENT_VALUES,
            'step' : DEFAULT_ADVANCEMENT_VALUES}

def updateFailSafe(data):
    return data['fail_safe']

def dateToTimestamp(date):
    if(date == None or date == ""):
        return 0
    
    format = "%Y-%m-%dT%H:%M:%S"
    date = date[0:DATE_STR_SIZE]
    date = datetime.datetime.strptime(date, format).replace(tzinfo=datetime.timezone.utc)

    return int(datetime.datetime.timestamp(date)) * FACT_SEC_TO_MIL

def updateDatePoint(data, field):
    return dateToTimestamp(data[field]) if field in data else 0

def updateSynchronizedAt(data):
    return updateDatePoint(data, 'synchronized_at')

def updateUpdatedAt(data):
    return updateDatePoint(data, 'updated_at')

def updateCreatedAt(data):
    return updateDatePoint(data, 'created_at')

def updateClosedAt(data):
    return updateUpdatedAt(data)

def updateVersionId(data):
    return data['version_id']

def updatePatientFirstName(data, param_data):
    study_data = param_data[getParamName(data)]
    first_name_node_id = str(study_data['first_name'])

    if('nodes' in data and first_name_node_id in data['nodes'].keys()):
        return data['nodes'][first_name_node_id]['value']
    else:
        return ""

def updatePatientLastName(data, param_data):
    study_data = param_data[getParamName(data)]
    last_name_node_id = str(study_data['last_name'])

    if('nodes' in data and last_name_node_id in data['nodes'].keys()):
        return data['nodes'][last_name_node_id]['value']
    else:
        return ""

def updatePatientBirthDate(data, param_data):
    study_data = param_data[getParamName(data)]
    birth_date_node_id = str(study_data['birth_date'])

    if('nodes' in data and birth_date_node_id in data['nodes'].keys()):
        return dateToTimestamp(data['nodes'][birth_date_node_id]['value'])
    else:
        return 0

def updatePatientBirthDateEstimated():
    return DEFAULT_BIRTH_DATE_ESTIMATED

def updatePatientBirthDateEstimatedType():
    return DEFAULT_BIRTH_DATE_ESTIMATED_TYPE

def updatePatientConsentFile(patient):
    return patient['consent_file']

def updatePatientConsent(patient):
    return updatePatientConsentFile(patient) != None

def updatePatientCreatedAt(patient):
    return dateToTimestamp(patient['created_at'])

def updatePatientFailSafe(patient):
    return patient['fail_safe']

def updatePatientGroupId(patient):
    return patient['group_id']

def updatePatientId(patient):
    return patient['id']

def updatePatientOtherGroupId(patient):
    return patient['other_group_id']

def updatePatientOtherStudyId(patient):
    return patient['other_study_id']

def updatePatientOtherUid(patient):
    return patient['other_uid']

def updatePatientReason(patient):
    return patient['reason']

def updatePatientStudyId(patient):
    return STUDY_TO_ID[patient['study_id']]

def updatePatientUid(patient):
    return patient['uid']

def updatePatientUpdatedAt(patient):
    return dateToTimestamp(patient['updated_at'])

def updatePatientPatientValues(patient):
    patient_values = patient['patientValues']
    new_patient_values = []
    for patient_value in patient_values:
        new_patient_values.append(patient_value)

    return new_patient_values

def updatePatientMedicalCases(patient):
    return patient['medicalCases']

def updatePatient(data, param_data):
    patient = data['patient']
    new_patient = {}
    new_patient['first_name']                   = updatePatientFirstName(data, param_data)
    new_patient['last_name']                    = updatePatientLastName(data, param_data)
    new_patient['birth_date_estimated']         = updatePatientBirthDateEstimated()
    new_patient['birth_date_estimated_type']    = updatePatientBirthDateEstimatedType()
    new_patient['birth_date']                   = updatePatientBirthDate(data, param_data)
    new_patient['consent']                      = updatePatientConsent(patient)
    new_patient['consent_file']                 = updatePatientConsentFile(patient)
    new_patient['createdAt']                    = updatePatientCreatedAt(patient)
    new_patient['fail_safe']                    = updatePatientFailSafe(patient)
    new_patient['group_id']                     = updatePatientGroupId(patient)
    new_patient['id']                           = updatePatientId(patient)
    new_patient['other_group_id']               = updatePatientOtherGroupId(patient)
    new_patient['other_study_id']               = updatePatientOtherStudyId(patient)
    new_patient['other_uid']                    = updatePatientOtherUid(patient)
    new_patient['reason']                       = updatePatientReason(patient)
    new_patient['study_id']                     = updatePatientStudyId(patient)
    new_patient['uid']                          = updatePatientUid(patient)
    new_patient['updatedAt']                    = updatePatientUpdatedAt(patient)
    if('patientValues' in patient):
        new_patient['patientValues']            = updatePatientPatientValues(patient)
    if('medicalCases' in patient):
        new_patient['medicalCases']             = updatePatientMedicalCases(patient)

    return new_patient

def processCase(data, param_data, version_json):
    new_data = {}

    new_data['id']              = updateId(data)
    new_data['activities']      = updateActivities(data, param_data)
    new_data['comment']         = updateComment(data)
    new_data['consent']         = updateConsent(data)
    new_data['diagnosis']       = updateDiagnosis(data, param_data, version_json)
    new_data['nodes']           = updateNodes(data)
    new_data['json']            = updateJson(data)
    new_data['json_version']    = updateJsonVersion()
    new_data['advancement']     = updateAdvancement()
    new_data['fail_safe']       = updateFailSafe(data)
    new_data['synchronizedAt']  = updateSynchronizedAt(data)
    new_data['updatedAt']       = updateUpdatedAt(data)
    new_data['createdAt']       = updateCreatedAt(data)
    new_data['closedAt']        = updateClosedAt(data)
    new_data['version_id']      = updateVersionId(data)
    new_data['patient']         = updatePatient(data, param_data)

    return new_data



#=== MAIN
path_to_cases = Path().resolve().joinpath("cases")
param_data = loadParameters()
checkDirectory(path_to_cases)
checkFiles(path_to_cases)

input(MSG_CONTINUE)
n_total, n_processed, n_failed, n_ignored = processCases(path_to_cases, param_data)

_file           = MSG_FILE_FOUND_FILE if n_total == 1 else MSG_FILE_FOUND_FILES
_ignored_file   = MSG_FILES_FOUND_FILE if n_ignored == 1 else MSG_FILES_FOUND_FILES
_processed_file = MSG_FILES_FOUND_FILE if n_processed == 1 else MSG_FILES_FOUND_FILES
_failed_file    = MSG_FILES_FOUND_FILE if n_failed == 1 else MSG_FILES_FOUND_FILES

print(MSG_FILES_INFO.format(n_total = n_total, n_processed = n_processed, n_failed = n_failed, n_ignored = n_ignored, _file=_file, _processed_file=_processed_file, _ignored_file=_ignored_file, _failed_file=_failed_file))

print(MSG_SUCCESSFUL)