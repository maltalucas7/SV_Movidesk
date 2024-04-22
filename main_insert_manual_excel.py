import pandas as pd
import requests
import ast
import numpy as np
import datetime
import time
import humanize
import os
import sys

def contador_regressivo(segundos):
    for i in range(segundos, 0, -1):
        sys.stdout.write(f"\rContagem regressiva: {i} ")
        sys.stdout.flush()  # Limpa o buffer de saída, forçando a atualização do texto no terminal
        time.sleep(1)  # Espera um segundo
    sys.stdout.write("\rContagem concluída!      \n")  # Limpa a linha após a conclusão


def get_results_tickets(proxies, start_date, end_date, page_size=20):
    # Constantes
    URL_BASE = "https://api.movidesk.com/public/v1/tickets"
    TOKEN = "4700e23f-d2dc-49fe-9411-fea63a4bc3cd"

    start_date_str = start_date.strftime('%Y-%m-%dT03:00:00.00z')
    end_date_str = end_date.strftime('%Y-%m-%dT02:59:59.00z')
    skip = 0
    all_tickets = []

    while True:
        params = {
            "token": TOKEN,
            "$filter": f"lastUpdate ge {start_date_str} and lastUpdate le {end_date_str}",
            "$select": "id",
            "$skip": skip,
            "$top": page_size
        }

        try:
            response = requests.get(URL_BASE, params=params, proxies=proxies)
            response.raise_for_status()  # Verifica se a resposta foi bem-sucedida
        except requests.RequestException as e:
            print(f"Erro na requisição: {e}")
            break

        data = response.json()
        if not data:
            break

        all_tickets.extend(data)
        skip += page_size

    list_tickets = []
    for item in all_tickets:
        ticket_id = item.get('id')
        if ticket_id:
            params = {"token": TOKEN, "id": ticket_id, "skip": skip, "top": page_size}
            while True:  # Loop infinito para continuar tentando até que seja bem-sucedido
                try:
                    response = requests.get(URL_BASE, params=params, proxies=proxies)
                    response.raise_for_status()  # Isso vai lançar uma exceção para respostas 4xx/5xx
                    list_tickets.append(response.json())
                    print(f"Sucesso na requisição do ticket {ticket_id}")
                    break  # Sai do loop após sucesso
                except requests.RequestException as e:
                    print(f"Incesucesso na requisição do ticket {ticket_id}")
                    contador_regressivo(60)  # Pausa a execução por 30 segundos antes de tentar novamente
    return list_tickets

def stringify_complex_columns(row):
    for idx, item in enumerate(row):
        if isinstance(item, (dict, list)):
            row.iloc[idx] = str(item)
    return row

def processar_intervalo(PROXY, START_DATE, END_DATE):
    start_date_dt = datetime.datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date_dt = datetime.datetime.strptime(END_DATE, "%Y-%m-%d") + datetime.timedelta(days=1)

    df_resultados = pd.DataFrame()

    tickets_detalhados = get_results_tickets(PROXY, start_date_dt, end_date_dt)
    
    if tickets_detalhados:
        df_tickets_detalhados = pd.DataFrame(tickets_detalhados)
                        
        df_tickets_detalhados = df_tickets_detalhados.apply(lambda row: stringify_complex_columns(row), axis=1)

        # Novo bloco de código para ajustar o fillna conforme o tipo de dados da coluna
        for col in df_tickets_detalhados.columns:
            if pd.api.types.is_numeric_dtype(df_tickets_detalhados[col]):
                df_tickets_detalhados[col].fillna(0, inplace=True)  # Para numéricos
            else:
                df_tickets_detalhados[col].fillna("", inplace=True)  # Para não numéricos

        if not df_tickets_detalhados.empty:

            # CORREÇÕES DE BASE --------------------------------------------------------------------------------------------

            df_tickets_detalhados['relativeTickets'] = np.where(df_tickets_detalhados['parentTickets'] == '[]', 'Pai', 'Filho')

            # Convertendo colunas de datas para datetime, ajustando o fuso horário e reformatando para string
            colunas_datas = ['createdDate', 'resolvedIn', 'closedIn', 'canceledIn', 'slaSolutionDate', 'lastActionDate', 'lastUpdate']
            for coluna in colunas_datas:
                # Convertendo para datetime com coerção para lidar com formatos inconsistentes ou valores inválidos
                df_tickets_detalhados[coluna] = pd.to_datetime(df_tickets_detalhados[coluna], errors='coerce')
                # Subtraindo 3 horas para ajustar o fuso horário
                df_tickets_detalhados[coluna] = df_tickets_detalhados[coluna] - pd.Timedelta(hours=3)
                # Reformatando para string no formato desejado, ignorando valores NaT resultantes de conversões falhas
                df_tickets_detalhados[coluna] = df_tickets_detalhados[coluna].dt.strftime('%d/%m/%Y %H:%M:%S').replace('NaT', '')


            def convert_to_dict(row):
                try:
                    if isinstance(row, dict):
                        return row
                    elif isinstance(row, str):
                        return ast.literal_eval(row)
                    else:
                        return {}
                except:
                    return {}
                
            df_tickets_detalhados['owner'] = df_tickets_detalhados['owner'].apply(convert_to_dict)
            owners_df = pd.json_normalize(df_tickets_detalhados['owner'])
            owners_df.columns = ['owner_' + col for col in owners_df.columns]
            df_tickets_detalhados = df_tickets_detalhados.join(owners_df)

            df_tickets_detalhados['createdBy'] = df_tickets_detalhados['createdBy'].apply(convert_to_dict)
            createdBy_df = pd.json_normalize(df_tickets_detalhados['createdBy'])
            createdBy_df.columns = ['createdBy_' + col for col in createdBy_df.columns]
            df_tickets_detalhados = df_tickets_detalhados.join(createdBy_df)
            
            df_tickets_detalhados['tags'] = df_tickets_detalhados['tags'].str.replace(r"[\[\]]", "", regex=True)
            df_tickets_detalhados['tags'] = df_tickets_detalhados['tags'].str.replace("'", "")
            df_tickets_detalhados['tags'] = df_tickets_detalhados['tags'].str.replace(" ", "")

            tags_expanded = df_tickets_detalhados['tags'].str.split(',', expand=True)

            for i in range(5):
                if i not in tags_expanded.columns:
                    tags_expanded[i] = None

            tags_expanded.columns = ['tag1', 'tag2', 'tag3', 'tag4', 'tag5']

            df_tickets_detalhados = df_tickets_detalhados.join(tags_expanded)

            # CLIENT

            def expand_clients_column(client_entry):
                if isinstance(client_entry, str):
                    try:
                        client_dict = ast.literal_eval(client_entry)
                        return client_dict
                    except ValueError:
                        return {}
                elif isinstance(client_entry, dict):
                    return client_entry
                else:
                    return {}

            if 'clients' in df_tickets_detalhados.columns:
                expanded_clients = df_tickets_detalhados['clients'].apply(expand_clients_column)
                expanded_clients_df = pd.DataFrame(expanded_clients.tolist())
                expanded_clients_df.columns = ['clients_' + str(col) for col in expanded_clients_df.columns]
                df_tickets_detalhados = pd.concat([df_tickets_detalhados.drop('clients', axis=1), expanded_clients_df], axis=1)

            def expand_clients_column(client_entry, col_prefix):
                client_dict = {}  # Definindo client_dict como um dicionário vazio por padrão

                if isinstance(client_entry, str):
                    try:
                        client_dict = ast.literal_eval(client_entry)
                    except ValueError:
                        pass
                elif isinstance(client_entry, dict):
                    client_dict = client_entry

                selected_keys = ['id', 'businessName', 'email', 'phone']
                client_dict_selected = {col_prefix + '_' + key: client_dict.get(key, '') for key in selected_keys}

                return pd.Series(client_dict_selected)

            for col_name in df_tickets_detalhados.columns:
                if col_name.startswith('clients_'):
                    expanded_clients = df_tickets_detalhados[col_name].apply(lambda x: expand_clients_column(x, col_name))
                    df_tickets_detalhados = pd.concat([df_tickets_detalhados, expanded_clients], axis=1).drop(col_name, axis=1)

            # PESQUISA DE SATISFAÇÃO

            def expand_satisfactionSurveyResponses_column(client_entry):
                if isinstance(client_entry, str):
                    try:
                        client_dict = ast.literal_eval(client_entry)
                        return client_dict
                    except ValueError:
                        return {}
                elif isinstance(client_entry, dict):
                    return client_entry
                else:
                    return {}

            if 'satisfactionSurveyResponses' in df_tickets_detalhados.columns:
                expanded_satisfactionSurveyResponses = df_tickets_detalhados['satisfactionSurveyResponses'].apply(expand_satisfactionSurveyResponses_column)
                expanded_satisfactionSurveyResponses_df = pd.DataFrame(expanded_satisfactionSurveyResponses.tolist())
                expanded_satisfactionSurveyResponses_df.columns = ['satisfactionSurveyResponses_' + str(col) for col in expanded_satisfactionSurveyResponses_df.columns]
                df_tickets_detalhados = pd.concat([df_tickets_detalhados.drop('satisfactionSurveyResponses', axis=1), expanded_satisfactionSurveyResponses_df], axis=1)

            def expand_satisfactionSurveyResponses_column(client_entry, col_prefix):
                client_dict = {}  # Definindo client_dict como um dicionário vazio por padrão

                if isinstance(client_entry, str):
                    try:
                        client_dict = ast.literal_eval(client_entry)
                    except ValueError:
                        pass
                elif isinstance(client_entry, dict):
                    client_dict = client_entry

                selected_keys = ['id', 'businessName', 'email', 'phone']
                client_dict_selected = {col_prefix + '_' + key: client_dict.get(key, '') for key in selected_keys}

                return pd.Series(client_dict_selected)

            for col_name in df_tickets_detalhados.columns:
                if col_name.startswith('satisfactionSurveyResponses_'):
                    expanded_satisfactionSurveyResponses = df_tickets_detalhados[col_name].apply(lambda x: expand_satisfactionSurveyResponses_column(x, col_name))
                    df_tickets_detalhados = pd.concat([df_tickets_detalhados, expanded_satisfactionSurveyResponses], axis=1).drop(col_name, axis=1)

            # PARENTS TICKETS
                    
            # Extrai apenas os IDs dos tickets filhos e os converte em uma string separada por vírgulas
            def extract_parent_ticket_ids(parent_tickets):
                if isinstance(parent_tickets, str):
                    try:
                        parent_tickets_list = ast.literal_eval(parent_tickets)
                        parent_ids = [str(child['id']) for child in parent_tickets_list if 'id' in child]
                        return ', '.join(parent_ids)  # Junta os IDs com vírgula como separador
                    except (ValueError, SyntaxError):
                        return ''
                return ''

            df_tickets_detalhados['parentTicketIds'] = df_tickets_detalhados['parentTickets'].apply(extract_parent_ticket_ids)


            # CHILDREN TICKETS
                    
            # Extrai apenas os IDs dos tickets filhos e os converte em uma string separada por vírgulas
            def extract_children_ticket_ids(children_tickets):
                if isinstance(children_tickets, str):
                    try:
                        children_tickets_list = ast.literal_eval(children_tickets)
                        children_ids = [str(child['id']) for child in children_tickets_list if 'id' in child]
                        return ', '.join(children_ids)  # Junta os IDs com vírgula como separador
                    except (ValueError, SyntaxError):
                        return ''
                return ''

            df_tickets_detalhados['childrenTicketIds'] = df_tickets_detalhados['childrenTickets'].apply(extract_children_ticket_ids)

            # CUSTOM FIELD

            custom_fields_descriptions = {
                                            93892:  "Autorizado_Por",
                                            153473: "Custo_do_Serviço_(Previsto)",
                                            93889:  "Custo_do_Serviço_(Real)",
                                            140568: "Data_de_Inicio_(Usina_Parada)",
                                            153480: "Data_de_Pagamento_(Entrada)",
                                            153593: "Data_Pagamento_(Final)",
                                            140569: "Data_de_Solução_(Usina_Parada)",
                                            106738: "Data_do_Proximo_Contato",
                                            95239:  "Equipe_Responsável",
                                            93890:  "Garantia",
                                            146754: "Justificativa_(Usina_Parada)",
                                            146010: "Motivo_de_Atraso_SLA",
                                            146011: "Motivo_Atraso_SLA_(Complemento)",
                                            146013: "Motivo_Orçamentos_Zerados/Fora_de_Margem",
                                            146014: "Motivo_Orçamentos_Zerados/Fora_de_Margem_(Complemento)",
                                            74474:  "Número_da_Pasta",
                                            158945: "Pasta_do_Chamado",
                                            140570: "Percentual_Desativado",
                                            114747: "Pesquisa_Enviada?",
                                            158591: "Status_Atual",
                                            146029: "Ticket_Take",
                                            93889:  "Custo_Do_Servico",
                                            153475: "Valor_Cobrado_do_Cliente_(Previsto)",
                                            93891:  "Valor_Cobrado_do_Cliente_(Real)",
                                            153478: "Valor_de_Entrada",
                                            153594: "Valor_Pagamento_Final",
                                            178948: "Custo_SV"
                                        }
                        
            def extract_custom_field_values(row, field_id):
                if row and isinstance(row, str):
                    try:
                        custom_fields = ast.literal_eval(row)
                        for field in custom_fields:
                            if field['customFieldId'] == field_id:
                                value = field.get('value') or (field.get('items')[0].get('customFieldItem') if field.get('items') else None)
                                # Verifica se o valor é 'n/a' ou 'N/A' e retorna vazio se for
                                if value in ['n/a', 'N/A', None]:
                                    return ''
                                else:
                                    return value
                    except:
                        pass
                return ''

            for field_id, desc in custom_fields_descriptions.items():
                column_name = f"{field_id}_{desc}"
                df_tickets_detalhados[column_name] = df_tickets_detalhados['customFieldValues'].apply(lambda row: extract_custom_field_values(row, field_id))

            df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'] = pd.to_datetime(df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'])
            df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'] = df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'] - pd.Timedelta(hours=3)
            df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'] = df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'].dt.strftime('%d/%m/%Y')

            df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'] = pd.to_datetime(df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'])
            df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'] = df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'] - pd.Timedelta(hours=3)
            df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'] = df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'].dt.strftime('%d/%m/%Y')

            df_tickets_detalhados['153593_Data_Pagamento_(Final)'] = pd.to_datetime(df_tickets_detalhados['153593_Data_Pagamento_(Final)'])
            df_tickets_detalhados['153593_Data_Pagamento_(Final)'] = df_tickets_detalhados['153593_Data_Pagamento_(Final)'] - pd.Timedelta(hours=3)
            df_tickets_detalhados['153593_Data_Pagamento_(Final)'] = df_tickets_detalhados['153593_Data_Pagamento_(Final)'].dt.strftime('%d/%m/%Y')

            df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'] = pd.to_datetime(df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'])
            df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'] = df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'] - pd.Timedelta(hours=3)
            df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'] = df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'].dt.strftime('%d/%m/%Y')

            df_tickets_detalhados['106738_Data_do_Proximo_Contato'] = pd.to_datetime(df_tickets_detalhados['106738_Data_do_Proximo_Contato'])
            df_tickets_detalhados['106738_Data_do_Proximo_Contato'] = df_tickets_detalhados['106738_Data_do_Proximo_Contato'] - pd.Timedelta(hours=3)
            df_tickets_detalhados['106738_Data_do_Proximo_Contato'] = df_tickets_detalhados['106738_Data_do_Proximo_Contato'].dt.strftime('%d/%m/%Y')

            df_tickets_detalhados['93891_Valor_Cobrado_do_Cliente_(Real)'] = df_tickets_detalhados['93891_Valor_Cobrado_do_Cliente_(Real)'].apply(lambda x: x.replace('R$', '').strip() if isinstance(x, str) else x)
            df_tickets_detalhados['153478_Valor_de_Entrada'] = df_tickets_detalhados['153478_Valor_de_Entrada'].apply(lambda x: x.replace('R$', '').strip() if isinstance(x, str) else x)
            df_tickets_detalhados['153594_Valor_Pagamento_Final'] = df_tickets_detalhados['153594_Valor_Pagamento_Final'].apply(lambda x: x.replace('R$', '').strip() if isinstance(x, str) else x)
            df_tickets_detalhados['93889_Custo_Do_Servico'] = df_tickets_detalhados['93889_Custo_Do_Servico'].apply(lambda x: x.replace('R$', '').strip() if isinstance(x, str) else x)


            # DROP COLUMNS

            df_tickets_detalhados = df_tickets_detalhados.drop(
                columns=['owner','createdBy','protocol','type','baseStatus','origin','originEmailAccount',
                        'serviceFull','serviceFirstLevelId','serviceSecondLevel','serviceThirdLevel',
                        'contactForm','tags','cc','actionCount','resolvedInFirstCall','chatWidget','chatGroup',
                        'chatTalkTime','chatWaitingTime','sequence','slaAgreement','slaAgreementRule',
                        'slaSolutionChangedByUser','slaSolutionChangedBy','slaSolutionDateIsPaused',
                        'jiraIssueKey','redmineIssueId','movideskTicketNumber','linkedToIntegratedTicketNumber',
                        'reopenedIn','slaResponseDate','slaRealResponseDate','ownerHistories',
                        'statusHistories','assets','webhookEvents','actions','childrenTickets','parentTickets',
                        'customFieldValues','owner_personType','owner_profileType','createdBy_personType',
                        'createdBy_profileType'
                        ])
                        
            # FIM DAS CORREÇÕES DE BASE ----------------------------------------------------------------------------------------

        df_resultados = pd.concat([df_resultados, df_tickets_detalhados], ignore_index=True)

    return df_resultados

def atualizar_ou_manter_tickets(df_novos_tickets, arquivo_excel):
    if os.path.exists(arquivo_excel):
        df_existente = pd.read_excel(arquivo_excel)

        # Garante que 'id' seja tratado como texto
        df_existente['id'] = df_existente['id'].astype(str)
        df_novos_tickets['id'] = df_novos_tickets['id'].astype(str)
        
        # Verifica se a coluna 'lastUpdate' existe. Se não, cria com valores NaT.
        if 'lastUpdate' not in df_existente.columns:
            df_existente['lastUpdate'] = pd.NaT  # Adiciona a coluna com valores padrão como Not a Time (NaT)

        # Converte 'lastUpdate' para datetime se ainda não estiver
        df_existente['lastUpdate'] = pd.to_datetime(df_existente['lastUpdate'], errors='coerce')
        df_novos_tickets['lastUpdate'] = pd.to_datetime(df_novos_tickets['lastUpdate'], errors='coerce', dayfirst=True)

        # Combina os DataFrames existente e novos tickets
        df_combinado = pd.concat([df_existente, df_novos_tickets])
        
        # Ordena por 'lastUpdate' para garantir que a última atualização fique por último
        df_combinado.sort_values(by='lastUpdate', ascending=False, inplace=True)
        
        # Remove duplicatas mantendo a última entrada baseada no ID
        df_atualizado = df_combinado.drop_duplicates(subset=['id'], keep='first')
    else:
        # Garante que 'id' seja tratado como texto antes de prosseguir
        df_novos_tickets['id'] = df_novos_tickets['id'].astype(str)
        df_atualizado = df_novos_tickets

    return df_atualizado
if __name__ == "__main__":
    PROXY = None
    START_DATE = "2000-01-01"
    END_DATE = "2024-12-31"
    #01-01-22 até 31-05-23 faltando

    start_time = time.time()
    df_final = processar_intervalo(PROXY, START_DATE, END_DATE)
    end_time = time.time()

    if not df_final.empty:
        arquivo_excel = 'Base_MD_SV.xlsx'
        df_atualizado = atualizar_ou_manter_tickets(df_final, arquivo_excel)
        df_atualizado.to_excel(arquivo_excel, index=False)
        print("Resultados salvos com sucesso no arquivo Excel.")
    else:
        print("Nenhum resultado encontrado para o intervalo especificado.")

    elapsed_time = end_time - start_time
    formatted_time = humanize.precisedelta(elapsed_time, minimum_unit="seconds", format="%0.0f")
    print(f"Tempo total de execução: {formatted_time}.")