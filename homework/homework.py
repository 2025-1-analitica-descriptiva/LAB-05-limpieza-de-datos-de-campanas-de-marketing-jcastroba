"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import pandas as pd
    import zipfile
    import io
    from glob import glob
    
    # Crear directorio de salida si no existe
    os.makedirs("files/output", exist_ok=True)
    
    # Buscar todos los archivos ZIP en files/input/
    zip_files = glob("files/input/*.zip")
    
    # Lista para almacenar todos los DataFrames
    all_dataframes = []
    
    # Procesar cada archivo ZIP
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            # Listar archivos CSV dentro del ZIP
            csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
            
            for csv_file in csv_files:
                # Leer CSV directamente desde el ZIP
                with zip_ref.open(csv_file) as file:
                    df = pd.read_csv(io.StringIO(file.read().decode('utf-8')))
                    all_dataframes.append(df)
    
    # Concatenar todos los DataFrames
    if all_dataframes:
        data = pd.concat(all_dataframes, ignore_index=True)
    else:
        # Si no hay archivos ZIP, intentar leer CSVs directos
        csv_files = glob("files/input/*.csv")
        if csv_files:
            data = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
        else:
            raise FileNotFoundError("No se encontraron archivos CSV o ZIP en files/input/")
    
    # Crear client_id si no existe
    if 'client_id' not in data.columns:
        data['client_id'] = range(len(data))
    
    # Procesar datos del cliente
    client_data = data[['client_id']].copy()
    
    # Agregar columnas del cliente con transformaciones
    if 'age' in data.columns:
        client_data['age'] = data['age']
    
    if 'job' in data.columns:
        client_data['job'] = data['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    
    if 'marital' in data.columns:
        client_data['marital'] = data['marital']
    
    if 'education' in data.columns:
        client_data['education'] = data['education'].str.replace('.', '_', regex=False)
        client_data['education'] = client_data['education'].replace('unknown', pd.NA)
    
    if 'default' in data.columns:
        client_data['credit_default'] = data['default'].apply(lambda x: 1 if x == 'yes' else 0)
    elif 'credit_default' in data.columns:
        client_data['credit_default'] = data['credit_default'].apply(lambda x: 1 if x == 'yes' else 0)
    
    if 'housing' in data.columns:
        client_data['mortgage'] = data['housing'].apply(lambda x: 1 if x == 'yes' else 0)
    elif 'mortgage' in data.columns:
        client_data['mortgage'] = data['mortgage'].apply(lambda x: 1 if x == 'yes' else 0)
    
    # Procesar datos de la campaña
    campaign_data = data[['client_id']].copy()
    
    if 'campaign' in data.columns:
        campaign_data['number_contacts'] = data['campaign']
    elif 'number_contacts' in data.columns:
        campaign_data['number_contacts'] = data['number_contacts']
    
    if 'duration' in data.columns:
        campaign_data['contact_duration'] = data['duration']
    elif 'contact_duration' in data.columns:
        campaign_data['contact_duration'] = data['contact_duration']
    
    if 'previous' in data.columns:
        campaign_data['previous_campaign_contacts'] = data['previous']
    elif 'previous_campaign_contacts' in data.columns:
        campaign_data['previous_campaign_contacts'] = data['previous_campaign_contacts']
    
    if 'poutcome' in data.columns:
        campaign_data['previous_outcome'] = data['poutcome'].apply(lambda x: 1 if x == 'success' else 0)
    elif 'previous_outcome' in data.columns:
        campaign_data['previous_outcome'] = data['previous_outcome'].apply(lambda x: 1 if x == 'success' else 0)
    
    if 'y' in data.columns:
        campaign_data['campaign_outcome'] = data['y'].apply(lambda x: 1 if x == 'yes' else 0)
    elif 'campaign_outcome' in data.columns:
        campaign_data['campaign_outcome'] = data['campaign_outcome'].apply(lambda x: 1 if x == 'yes' else 0)
    
    # Crear fecha de último contacto
    if 'day' in data.columns and 'month' in data.columns:
        campaign_data['last_contact_date'] = pd.to_datetime(
            '2022-' + data['month'].astype(str).str.zfill(2) + '-' + data['day'].astype(str).str.zfill(2)
        ).dt.strftime('%Y-%m-%d')
    elif 'last_contact_date' in data.columns:
        campaign_data['last_contact_date'] = data['last_contact_date']
    
    # Procesar datos económicos
    economics_data = data[['client_id']].copy()
    
    if 'cons.price.idx' in data.columns:
        economics_data['cons_price_idx'] = data['cons.price.idx']
    elif 'cons_price_idx' in data.columns:
        economics_data['cons_price_idx'] = data['cons_price_idx']
    
    if 'euribor3m' in data.columns:
        economics_data['euribor_three_months'] = data['euribor3m']
    elif 'euribor_three_months' in data.columns:
        economics_data['euribor_three_months'] = data['euribor_three_months']
    
    # Guardar archivos CSV
    client_data.to_csv('files/output/client.csv', index=False)
    campaign_data.to_csv('files/output/campaign.csv', index=False)
    economics_data.to_csv('files/output/economics.csv', index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()