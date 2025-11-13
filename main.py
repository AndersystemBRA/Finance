########################## CLOUD FUNCTION PYTHON (desenvolvida pela area de BI DFG)
"""
DEVS
[jose_junior_30@carrefour.com ,
anderson_ferreira_1@carrefour.com ,
rafael_cardoso@carrefour.com ,
david_barbato@carrefour.com]
"""

from google.cloud import storage, bigquery
import re
from datetime import datetime
from io import StringIO
import tempfile


# Definindo nomes e variáveis
bucket_name = 'br-apps-property-indicator-prd-cs-001'
folder_name = 'DADOS'
project_id = 'br-apps-property-indicator-prd'
dataset_name = 'Imports'
log_dataset_name = 'LOG'
target_folder = 'BI_DFG_FILES'
success_folder = 'sucesso'
failure_folder = 'falha'


# Variável para armazenar logs de execução
execution_logs = set()


# Função de log customizada para armazenar logs
# Função de log customizada para armazenar logs
def log(message):
    print(message)
    execution_logs.add(message)  # Adiciona a mensagem ao set


# Função para obter arquivos .txt tabulados por |
def get_txt_files(bucket_name, folder_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f"{folder_name}/")  # garante que só venha de SAP/
    
    txt_files = [
        blob.name for blob in blobs
        if blob.name.endswith('.txt') and blob.name.startswith(f"{folder_name}/")
    ]
    return txt_files

# Função para verificar e criar estrutura de pastas
def ensure_folder_structure(bucket_name, target_folder, success_folder, failure_folder):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    target_blob = bucket.blob(target_folder)
    success_blob = bucket.blob(f"{target_folder}/{success_folder}/")
    failure_blob = bucket.blob(f"{target_folder}/{failure_folder}/")

    if not target_blob.exists():
        target_blob.upload_from_string('')
        log(f"Pasta {target_folder} criada.")

    if not success_blob.exists():
        success_blob.upload_from_string('')
        log(f"Pasta {success_folder} criada dentro de {target_folder}.")

    if not failure_blob.exists():
        failure_blob.upload_from_string('')
        log(f"Pasta {failure_folder} criada dentro de {target_folder}.")


# Função para criar dataset se não existir
def create_dataset_if_not_exists(dataset_name):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_name)
    try:
        client.get_dataset(dataset_ref)
        #log(f"Dataset {dataset_name} já existe.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        log(f"Dataset {dataset_name} criado.")

# Função para verificar se a tabela existe no BigQuery
def check_table_exists(project_id, dataset_name, table_name):
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    try:
        client.get_table(table_id)
        return True
    except Exception as e:
        return False

# Função para criar tabela no BigQuery se não existir
def create_table_if_not_exists(project_id, dataset_name, table_name, schema):
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    
    if not check_table_exists(project_id, dataset_name, table_name):
        try:
            table = bigquery.Table(table_id, schema=schema)
            # Define a partição pela coluna PARTITIONDATE
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="PARTITIONDATE"
            )
            client.create_table(table)
            log(f"Tabela {table_id} criada.")
        except Exception as e:
            log(f"##FALHA## Erro ao criar tabela {table_id}: {e}")

# Função para sanitizar os nomes das colunas
#def sanitize_column_name(name):
    # Substitui caracteres inválidos por sublinhado
 #   name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    #'/[^a-zA-Z0-9_]/' no PHP
    # Certifica-se de que o nome comece com uma letra ou sublinhado 
  #  if not re.match(r'^[a-zA-Z_]', name):
    #if re.match(r'^[0-9]', sanitized): No PHP
   #     name = '_' + name
    # Limita o comprimento a 128 caracteres (limite do BigQuery)
    #return name[:128]

# Função para sanitizar os nomes das colunas
def sanitize_column_name(name):
    # Substitui os espaços por _
    name = name.replace(' ', '_')
    # Remove espaços em branco no início e no fim (trim)
    #name = name.strip()
    # Substitui caracteres inválidos por sublinhado
    name = re.sub(r'[^a-zA-Z0-9_]', '__', name) #Para ficar identico ao PHP 
    #name = re.sub(r'[^a-zA-Z0-9_]+', '_', name) #Para apenas um _ para caracter especial
    #name = re.sub(r'[^a-zA-Z0-9_]', '_', name) #Para apenas um _ para cada caracter especial
    #'/[^a-zA-Z0-9_]/' no PHP
    # Certifica-se de que o nome comece com uma letra ou sublinhado
    if re.match(r'^[0-9]', name):
    #if re.match(r'^[0-9]', sanitized): No PHP
        name = '_' + name
    # Limita o comprimento a 128 caracteres (limite do BigQuery)
    return name[:128]


# Função para lidar com colunas duplicadas
def handle_duplicate_columns(columns):
    column_count = {}
    new_columns = []
    for col in columns:
        sanitized_col = sanitize_column_name(col)  # Função para ajustar o nome da coluna, se necessário
        if sanitized_col in column_count:
            column_count[sanitized_col] += 1
            sanitized_col = f"{sanitized_col}{column_count[sanitized_col] + 1}"  # Inicia a contagem no 2
        else:
            column_count[sanitized_col] = 0
        new_columns.append(sanitized_col)
    return new_columns

# Função para encontrar o header válido
def find_valid_header(content):
    # Verifica se uma linha tem colunas vazias
    def is_valid_header(line):
        return all(col.strip() != '' for col in line.split('|'))

    # Itera pelas linhas para encontrar um header válido
    lines = content.split('\n')
    for line in lines:
        if is_valid_header(line):
            return line.strip().split('|')
    
    return []  # Retorna uma lista vazia caso não encontre um header válido


# Função para criar tabelas particionadas e inserir dados
def create_partitioned_tables_and_insert_data(txt_files, project_id, dataset_name, bucket_name, target_folder, success_folder, failure_folder):
    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Regex que valida nomes no formato:
    # nome sem underline (apenas letras, números e espaços) + _ + 8 dígitos (YYYYMMDD) + .txt
    padrao_nome = re.compile(r'^[A-Za-z0-9]+(?: [A-Za-z0-9]+)*_\d{8}\.txt$')

    # Função auxiliar para mover arquivo para a pasta de falha
    def move_file_to_failure(file):
        failure_path = f"{target_folder}/{failure_folder}/{file.split('/')[-1]}"
        blob = bucket.blob(file)
        bucket.rename_blob(blob, failure_path)
        log(f"Arquivo {file} movido para {failure_path}.")
        return failure_path

    # Função auxiliar para remover arquivo temporário do Cloud Storage
    def remove_temp_file(file):
        temp_path = f"{target_folder}/temp/{file.split('/')[-1]}"
        bucket.delete_blob(temp_path)
        log(f"Arquivo {file} removido da pasta temp.")

    for file in txt_files:
        file_name = file.split('/')[-1]

        # Validação do padrão do nome do arquivo
        if not padrao_nome.match(file_name):
            log(f"##FALHA## Nome do arquivo fora do padrão: {file}. Movendo para a pasta de falha.")
            move_file_to_failure(file)
            continue

        table_name, date_str = file.split('/')[-1].split('_')
        table_name = table_name.replace(' ', '_')  # Substituir espaços por '_'
        date_str = date_str.replace('.txt', '')
        partition_date = datetime.strptime(date_str, '%Y%m%d').date()
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        
        # Criação do schema com colunas de string
        blob = bucket.blob(file)
        encoding = 'utf-8'
        try:
            content = blob.download_as_text(encoding=encoding)
        except UnicodeDecodeError:
            encoding = 'ISO-8859-1'
            content = blob.download_as_text(encoding=encoding)
                
        # Remover BOM se presente
        content = content.lstrip('\ufeff')
        
        # Validação: primeira linha em branco
        first_line_raw = content.splitlines()[0].strip() if content.splitlines() else ""
        if not first_line_raw:
            log(f"##FALHA## Primeira linha em branco no arquivo {file}. Movendo para a pasta de falha.")
            move_file_to_failure(file)
            continue
        
        # Função mais colunas
        linhas_excedentes = tem_colunas_excedentes(content)
        if linhas_excedentes:
            log(f"##FALHA## Linhas com mais colunas que o header detectadas no arquivo {file}. "
                f"Problema encontrado nas linhas: {linhas_excedentes}. Movendo para pasta de falha.")
            move_file_to_failure(file)
            continue

              
        # Função para substituir caracteres inválidos e remover quebras de linha dentro de aspas
        content = replace_invalid_characters(content)

        # Processamento das colunas usando find_valid_header para garantir um header válido
        first_line = find_valid_header(content)
        if not first_line:
            log(f"##FALHA## Header inválido encontrado no arquivo {file}.")
            move_file_to_failure(file)
            continue
        
        columns = handle_duplicate_columns(first_line)
        
        schema = [
            bigquery.SchemaField('PARTITIONDATE', 'DATE'),
            *[bigquery.SchemaField(col, "STRING") for col in columns]
        ]


        # Mover arquivo para pasta de falha antes de tentar inserção
        failure_path = move_file_to_failure(file)

        # Verifica se a tabela já existe e cria se necessário
        create_table_if_not_exists(project_id, dataset_name, table_name, schema)
        
        # Verifica se a tabela foi criada com sucesso antes de continuar
        if not check_table_exists(project_id, dataset_name, table_name):
            log(f"##FALHA## Tabela {table_id} não foi criada. Verifique os logs para mais detalhes.")
            continue

        # Compara as colunas do arquivo com as colunas da tabela
        if compare_columns(file, columns, table_id):
            # Sempre deleta a partição antes de inserir (idempotência garantida)
            delete_existing_partition_data(client, table_id, partition_date)
            log(f"Dados com PARTITIONDATE {partition_date} removidos de {table_id} (se existiam).")
                
            # Prepara os dados para inserção usando tempfile
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            with temp_file as tf:
                tf.write(f"PARTITIONDATE|{first_line}\n".encode('utf-8'))
                for line in content.split('\n')[1:]:
                    if line.strip():
                        tf.write(f"{partition_date}|{line}\n".encode('utf-8'))
                tf.seek(0)  # Move o cursor para o início do arquivo temporário

                temp_blob = bucket.blob(f"{target_folder}/temp/{file.split('/')[-1]}")
                temp_blob.upload_from_filename(tf.name)
            
            # Configura carga de dados para BigQuery
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.CSV,
                field_delimiter='|',
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            # Carrega os dados do arquivo temporário diretamente para o BigQuery
            try:
                load_job = client.load_table_from_uri(
                    f"gs://{bucket_name}/{target_folder}/temp/{file.split('/')[-1]}",
                    table_id,
                    job_config=job_config
                )
                log(f"Carregamento de dados para {table_id} iniciado.")
                load_job.result()  # Espera o carregamento ser concluído
                log(f"##SUCESSO## Arquivo {file} carregado para tabela {table_id} com sucesso.")
                
                # Mover arquivo para pasta de sucesso após inserção bem-sucedida
                success_path = f"{target_folder}/{success_folder}/{table_name}/{file.split('/')[-1]}"
                bucket.rename_blob(bucket.blob(failure_path), success_path)
                log(f"Arquivo {file} movido para {success_path}.")
                
                # Remove arquivo da pasta temp após sucesso
                remove_temp_file(file)
                
            except Exception as e:
                log(f"##FALHA## Erro ao carregar dados para {table_id}: {str(e)}")
                remove_temp_file(file)
        else:
            log(f"##FALHA## Esquema do arquivo {file} não corresponde ao da tabela {table_id}.")
            remove_temp_file(file)
            

# Função para excluir dados com a mesma PARTITIONDATE
def delete_existing_partition_data(client, table_id, partition_date):
    query = f"""
        DELETE FROM `{table_id}`
        WHERE PARTITIONDATE = DATE('{partition_date}')
    """
    query_job = client.query(query)
    query_job.result()  # Espera a execução da query ser concluída
    
    
# Função para detectar linhas com mais colunas que o header
def tem_colunas_excedentes(content):
    linhas = [l.strip() for l in content.splitlines() if l.strip()]
    if not linhas:
        return []  # Arquivo vazio → sem problemas

    num_cols_header = linhas[0].count('|') + 1
    linhas_com_problema = []

    for idx, linha in enumerate(linhas[1:], start=2):  # começa do 2 pois a linha 1 é o cabeçalho
        num_cols_linha = linha.count('|') + 1
        if num_cols_linha > num_cols_header:
            linhas_com_problema.append(idx)

    return linhas_com_problema

    

# Função para substituir caracteres inválidos e remover quebras de linha
def replace_invalid_characters(content):
    content = content.replace('"', '')
    linhas = content.splitlines()
    header = linhas[0].strip()
    num_separadores = header.count("|")

    texto_corrigido = content  # Inicializa com o texto original
    while True:
        linhas = texto_corrigido.splitlines()
        linhas_corrigidas = []
        linha_pendente = ""
        mudou = False  # Flag para verificar se houve alguma mudança

        for linha in linhas:
            linha = linha.strip()

            if not linha:
                continue

            if linha_pendente:
                linha = linha_pendente + linha
                linha_pendente = ""

            if linha.count("|") == num_separadores:
                linhas_corrigidas.append(linha)
            else:
                linha_pendente = linha + " "
                mudou = True  # Indica que uma linha foi quebrada e precisa ser corrigida

        if linha_pendente:
            linhas_corrigidas.append(linha_pendente)
            mudou = True  # Garante que a última linha pendente seja considerada

        texto_corrigido = "\n".join(linhas_corrigidas)

        if not mudou:
            break  # Se não houve mudanças, todas as quebras foram corrigidas
    
    content = content.replace('"', '') 
    return texto_corrigido
           
           
# Função para comparar colunas do arquivo com a tabela no BigQuery
def compare_columns(file, columns, table_id):
    client = bigquery.Client()
    table = client.get_table(table_id)

    # Remove PARTITIONDATE se existir
    table_columns = [field.name for field in table.schema if field.name != 'PARTITIONDATE']
    file_columns = [sanitize_column_name(col) for col in columns]

    # Se forem exatamente iguais
    if file_columns == table_columns:
        log(f"Tabela e arquivo possuem o mesmo schema para {table_id}")
        return True

    comparison_lines = ["Tabela|Arquivo|Status"]

    i = j = 0
    while i < len(table_columns) or j < len(file_columns):
        tabela_col = table_columns[i] if i < len(table_columns) else ''
        arquivo_col = file_columns[j] if j < len(file_columns) else ''

        if tabela_col == arquivo_col:
            comparison_lines.append(f"{tabela_col}|{arquivo_col}|✅OK")
            i += 1
            j += 1

        elif tabela_col and tabela_col not in file_columns[j:]:
            # Coluna da tabela não existe mais no arquivo
            comparison_lines.append(f"{tabela_col}||❌Erro schema campo ausente")
            i += 1

        elif arquivo_col and arquivo_col not in table_columns[i:]:
            # Coluna extra no arquivo
            comparison_lines.append(f"|{arquivo_col}|🆕Nova coluna")
            j += 1

        else:
            # Colunas diferentes no mesmo ponto
            comparison_lines.append(f"{tabela_col}|{arquivo_col}|❌Erro schema coluna com nomes diferentes")
            i += 1
            j += 1

    # Monta log formatado
    comparison_text = "\n".join(comparison_lines)
    log(f"\n{comparison_text}")

    # ---- REGRAS DE ALTERAÇÃO DE SCHEMA ----
    table_columns_set = set(table_columns)
    file_columns_set = set(file_columns)

    if table_columns_set.issubset(file_columns_set):
        file_columns_subset = [col for col in file_columns if col in table_columns_set]
        if file_columns_subset == table_columns:
            new_columns = [col for col in file_columns if col not in table_columns_set]
            if new_columns:
                last_table_col_index = file_columns.index(table_columns[-1])
                first_new_col_index = file_columns.index(new_columns[0])
                if first_new_col_index > last_table_col_index:
                    new_fields = [bigquery.SchemaField(col, "STRING") for col in new_columns]
                    updated_schema = table.schema + new_fields
                    table.schema = updated_schema
                    client.update_table(table, ["schema"])
                    log(f"Novas colunas {new_columns} adicionadas à tabela {table_id}.")
                    return True
                else:
                    log(f"ERRO: Coluna nova encontrada no meio da estrutura. "
                        f"As novas colunas devem estar após {table_columns[-1]}. Nenhuma alteração realizada.")
                    return False
            return True

    log("ERRO: Divergência de schema detectada. Nenhuma alteração realizada.")
    return False

# Função para salvar logs no BigQuery
def save_logs_to_bigquery(execution_logs, project_id, log_dataset_name, log_table_name):
    client = bigquery.Client()
    log_table_id = f"{project_id}.{log_dataset_name}.{log_table_name}"

    rows_to_insert = [
        {"Data": datetime.now().isoformat(), "TXT": log}
        for log in execution_logs  # Uso do set evita duplicação
    ]

    # Verifica se a tabela de logs existe, se não, cria a tabela
    if not check_table_exists(project_id, log_dataset_name, log_table_name):
        schema = [
            bigquery.SchemaField("Data", "TIMESTAMP"),
            bigquery.SchemaField("TXT", "STRING"),
        ]
        table = bigquery.Table(log_table_id, schema=schema)
        client.create_table(table)
        log(f"Tabela {log_table_id} criada para armazenar logs.")

    errors = client.insert_rows_json(log_table_id, rows_to_insert)
    if errors == []:
        print(f"Logs salvos com sucesso na tabela {log_table_id}.")
    else:
        print(f"Erros ao salvar logs: {errors}")

        
        


# NOVO: Importar Flask
from flask import Flask, request

# NOVO: Instanciar o aplicativo Flask
app = Flask(__name__)

# NOVO: Função para salvar logs no BigQuery (MANTENHA A SUA FUNÇÃO, apenas re-listada aqui por clareza)
# def save_logs_to_bigquery(...):
#     ...

# Função principal (main) adaptada para ser um endpoint Flask
@app.route('/', methods=['GET', 'POST'])
def process_data():
    global execution_logs # Garante que as funções internas possam preencher o set
    execution_logs = set() # Limpa logs a cada nova requisição
    
    # Simula o comportamento de uma Cloud Function, chamando a lógica principal
    try:
        # Chama a função principal que contém toda a sua lógica
        # Note: Você está passando o 'request' do Flask para a sua função principal.
        # Sua função principal não usa o objeto request, mas mantemos o nome para consistência.
        # Se for acionado via GET, request.get_json() falhará, mas sua função
        # principal nem precisa disso, pois ela varre o bucket.
        
        log("Execução iniciada.")
        create_dataset_if_not_exists(dataset_name)
        create_dataset_if_not_exists(log_dataset_name)
        ensure_folder_structure(bucket_name, target_folder, success_folder, failure_folder)

        # Busca arquivos .txt na pasta
        txt_files = get_txt_files(bucket_name, folder_name)

        if not txt_files:
            log(f"Nenhum arquivo .txt encontrado na pasta {folder_name}. Encerrando execução.")
            save_logs_to_bigquery(execution_logs, project_id, log_dataset_name, "LOGS")
            log("Execução concluída.")
            return "Nenhum arquivo encontrado. Execução concluída.", 200

        create_partitioned_tables_and_insert_data(
            txt_files,
            project_id,
            dataset_name,
            bucket_name,
            target_folder,
            success_folder,
            failure_folder
        )

        save_logs_to_bigquery(execution_logs, project_id, log_dataset_name, "LOGS")
        log("Execução concluída.")
        return "Execução finalizada com sucesso", 200

    except Exception as e:
        log(f"##FALHA GERAL## Erro na execução principal: {str(e)}")
        save_logs_to_bigquery(execution_logs, project_id, log_dataset_name, "LOGS")
        return f"Erro na execução principal: {str(e)}", 500

# NOVO: Ponto de entrada do Gunicorn (usado pelo Buildpack)
if __name__ == "__main__":
    # Esta parte só é útil para teste local, não é usada no Cloud Run
    # O Gunicorn usará o objeto 'app' definido acima
    app.run(debug=True, host="0.0.0.0", port=8080)

    #Gerar
    #pip freeze > requirements.txt
    #google-cloud-storage
    #google-cloud-bigquery
