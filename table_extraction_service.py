import json
import os

from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.utils import secure_filename

import TableExtractionPdfplumber as plumber
import TableExtractionTabula as tabula
import Utils
import Config as cfg

ALLOWED_EXTENSIONS = set(['pdf'])
UPLOAD_FOLDER = './data/uploads/'
#UPLOAD_FOLDER = 'D:\\DRA\python Code\\pdf_table_extractor\\pdf_table_extractor\\data\\uploads'
app = Flask(__name__)
CORS(app)
LIB_TABULA = 'tabula'
LIB_PLUMBER = 'plumber'
config_dict = {}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/extract', methods=['POST'])
def extractTables():
    lib = request.args.get('offerings')
    #print(lib)
    libs = lib.split(",")
    #print(libs)
    lattice_bool_value = False
    stream_bool_value = False
    guess_bool_value = False
    clean_empty_columns_bool_value = False
    table_dict = {}
    global config_dict
    config_dict = {}
    if request.method == 'POST':
        if 'file' not in request.files:
            print('No files')
            # redirect(request.url)
            return jsonify(upload='No files selected')
        file = request.files['file']
        
        if 'tabula_lattice' in request.form:
            if request.form['tabula_lattice'].lower() == 'true':
                    lattice_bool_value = True
        else:
            lattice_bool_value = cfg.lattice
            
        if 'tabula_stream' in request.form:
            if request.form['tabula_stream'].lower() == 'true':
                    stream_bool_value = True
        else:
            stream_bool_value = cfg.stream
                
        if 'tabula_guess' in request.form:
            if request.form['tabula_guess'].lower() == 'true':
                    guess_bool_value = True
        else:
            guess_bool_value = cfg.guess
                
        if 'tabula_encoding' in request.form:
            tabula_encoding = request.form['tabula_encoding']
        else:
            tabula_encoding = cfg.encoding
                
        if 'tabula_clean_empty_columns' in request.form:
            if request.form['tabula_clean_empty_columns'].lower() == 'true':
                clean_empty_columns_bool_value = True
        else:
            clean_empty_columns_bool_value = cfg.clean_empty_columns
    
        config_dict['tabula_lattice'] = lattice_bool_value
        config_dict['tabula_stream'] = stream_bool_value
        config_dict['tabula_guess'] = guess_bool_value
        config_dict['tabula_encoding'] = tabula_encoding
        config_dict['tabula_clean_empty_columns'] = clean_empty_columns_bool_value
        
    
    if file:
        if allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filePath = os.path.join(UPLOAD_FOLDER, filename)
            file.save(filePath)
            print(filePath)
            file.close()

            # call tablula or pdfplumber method
            for lib in libs:
                lib = lib.strip()
                if lib is not None and lib is not '':
                    if lib == LIB_TABULA:
                        table_dict['tabula'] = Utils.export_combined_json(get_tables(filePath, lib=LIB_TABULA), config_dict)
                    elif lib == LIB_PLUMBER:
                        table_dict['plumber'] = Utils.export_json(get_tables(filePath, lib=LIB_PLUMBER))
        else:
            return jsonify(upload='File is not in PDF format')
        print(table_dict)
        table_json = json.dumps(table_dict)
        # return redirect(url_for('uploaded_file'),filename=filename)
    os.remove(filePath)
    return table_json


def get_tables(filepath, lib=LIB_TABULA):
    #table_list = []
    print(lib)
    print(os.path.abspath(filepath))
    if lib == LIB_TABULA:
        table_dict = tabula.getTablesFromPdf(filepath, config_dict)
    elif lib == LIB_PLUMBER:
        table_dict = plumber.extract_using_pdftables(filepath)

    return table_dict
    # if table_dict:
    #     table_json=Utils.export_json(table_list)
    #     return table_json
    # else:
    #     return jsonify(tables=False)


if __name__ == '__main__':
    app.run(host='localhost', port=6501, debug=True)
    #extractTables()
