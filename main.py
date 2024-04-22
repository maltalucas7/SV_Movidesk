import os
import mysql.connector
from mysql.connector import Error
import pandas as pd
import requests
import ast
import numpy as np
import datetime
import time
import humanize
import sys
from datetime import datetime, timedelta

def contador_regressivo(segundos):
    for i in range(segundos, 0, -1):
        sys.stdout.write(f"\rContagem regressiva: {i} ")
        sys.stdout.flush()  # Limpa o buffer de saída, forçando a atualização do texto no terminal
        time.sleep(1)  # Espera um segundo
    sys.stdout.write("\rContagem concluída!      \n")  # Limpa a linha após a conclusão


def get_results_tickets(proxies, start_date, end_date, page_size=20):
    # Constantes
    URL_BASE = os.getenv('acess_URL_BASE')
    TOKEN = os.getenv('acess_TOKEN')

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
    start_date_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date_dt = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

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
                                            178948: "Custo_SV",
                                            92564:  "Detalhamento_Servico"
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

            df_tickets_detalhados['createdDate'] = pd.to_datetime(df_tickets_detalhados['createdDate'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_tickets_detalhados['lastUpdate'] = pd.to_datetime(df_tickets_detalhados['lastUpdate'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_tickets_detalhados['resolvedIn'] = pd.to_datetime(df_tickets_detalhados['resolvedIn'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_tickets_detalhados['canceledIn'] = pd.to_datetime(df_tickets_detalhados['canceledIn'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_tickets_detalhados['closedIn'] = pd.to_datetime(df_tickets_detalhados['closedIn'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_tickets_detalhados['lastActionDate'] = pd.to_datetime(df_tickets_detalhados['lastActionDate'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_tickets_detalhados['slaSolutionDate'] = pd.to_datetime(df_tickets_detalhados['slaSolutionDate'], dayfirst=True).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            df_tickets_detalhados['93889_Custo_Do_Servico'] = df_tickets_detalhados['93889_Custo_Do_Servico'].replace('', None)
            df_tickets_detalhados['153473_Custo_do_Serviço_(Previsto)'] = df_tickets_detalhados['153473_Custo_do_Serviço_(Previsto)'].replace('', None)
            df_tickets_detalhados['93891_Valor_Cobrado_do_Cliente_(Real)'] = df_tickets_detalhados['93891_Valor_Cobrado_do_Cliente_(Real)'].replace('', None)
            df_tickets_detalhados['153478_Valor_de_Entrada'] = df_tickets_detalhados['153478_Valor_de_Entrada'].replace('', None)
            df_tickets_detalhados['153594_Valor_Pagamento_Final'] = df_tickets_detalhados['153594_Valor_Pagamento_Final'].replace('', None)
            df_tickets_detalhados['178948_Custo_SV'] = df_tickets_detalhados['178948_Custo_SV'].replace('', None)
            df_tickets_detalhados['153475_Valor_Cobrado_do_Cliente_(Previsto)'] = df_tickets_detalhados['153475_Valor_Cobrado_do_Cliente_(Previsto)'].replace('', None)

            def convert_date(date):
                try:
                    return pd.to_datetime(date, format='%d/%m/%Y').strftime('%Y-%m-%d')
                except:
                    return None
                
            def convert_decimal(value):
                try:
                    return float(value.replace(',', '.'))
                except ValueError:
                    return value
                except AttributeError:
                    return value


            df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'] = df_tickets_detalhados['140568_Data_de_Inicio_(Usina_Parada)'].apply(convert_date)
            df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'] = df_tickets_detalhados['153480_Data_de_Pagamento_(Entrada)'].apply(convert_date)
            df_tickets_detalhados['153593_Data_Pagamento_(Final)'] = df_tickets_detalhados['153593_Data_Pagamento_(Final)'].apply(convert_date)
            df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'] = df_tickets_detalhados['140569_Data_de_Solução_(Usina_Parada)'].apply(convert_date)
            df_tickets_detalhados['106738_Data_do_Proximo_Contato'] = df_tickets_detalhados['106738_Data_do_Proximo_Contato'].apply(convert_date)

            df_tickets_detalhados['153475_Valor_Cobrado_do_Cliente_(Previsto)'] = df_tickets_detalhados['153475_Valor_Cobrado_do_Cliente_(Previsto)'].apply(convert_decimal)
            df_tickets_detalhados['93891_Valor_Cobrado_do_Cliente_(Real)'] = df_tickets_detalhados['93891_Valor_Cobrado_do_Cliente_(Real)'].apply(convert_decimal)
            df_tickets_detalhados['153478_Valor_de_Entrada'] = df_tickets_detalhados['153478_Valor_de_Entrada'].apply(convert_decimal)
            df_tickets_detalhados['153594_Valor_Pagamento_Final'] = df_tickets_detalhados['153594_Valor_Pagamento_Final'].apply(convert_decimal)
            df_tickets_detalhados['178948_Custo_SV'] = df_tickets_detalhados['178948_Custo_SV'].apply(convert_decimal)
            df_tickets_detalhados['93889_Custo_Do_Servico'] = df_tickets_detalhados['93889_Custo_Do_Servico'].apply(convert_decimal)

            df_tickets_detalhados.replace({np.nan: None}, inplace=True)

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
        
        renomear_colunas = {
            'id': 'id',
            'subject': 'subject',
            'serviceFirstLevel': 'service_first_level',
            'category': 'category',
            'urgency': 'urgency',
            'status': 'status',
            'justification': 'justification',
            'isDeleted': 'is_deleted',
            'createdDate': 'created_date',
            'lastUpdate': 'last_update',
            'resolvedIn': 'resolved_in',
            'canceledIn': 'canceled_in',
            'closedIn': 'closed_in',
            'ownerTeam': 'owner_team',
            'owner_businessName': 'owner_business_name',
            'lifeTimeWorkingTime': 'life_time_working_time',
            'stoppedTime': 'stopped_time',
            'stoppedTimeWorkingTime': 'stopped_time_working_time',
            'slaSolutionTime': 'sla_solution_time',
            'slaResponseTime': 'sla_response_time',
            'slaSolutionDate': 'sla_solution_date',
            'lastActionDate': 'last_action_date',
            'relativeTickets': 'relative_tickets',
            'tag1': 'tag1',
            'tag2': 'tag2',
            'tag3': 'tag3',
            'tag4': 'tag4',
            'tag5': 'tag5',
            '93892_Autorizado_Por': 'authorized_by',
            '153473_Custo_do_Serviço_(Previsto)': 'estimated_service_cost',
            '93889_Custo_Do_Servico': 'service_cost',
            '140568_Data_de_Inicio_(Usina_Parada)': 'plant_downtime_start_date',
            '153480_Data_de_Pagamento_(Entrada)': 'payment_entry_date',
            '153593_Data_Pagamento_(Final)': 'final_payment_date',
            '140569_Data_de_Solução_(Usina_Parada)': 'plant_downtime_solution_date',
            '106738_Data_do_Proximo_Contato': 'next_contact_date',
            '95239_Equipe_Responsável': 'responsible_team',
            '93890_Garantia': 'warranty',
            '146754_Justificativa_(Usina_Parada)': 'plant_downtime_justification',
            '146010_Motivo_de_Atraso_SLA': 'sla_delay_reason',
            '146011_Motivo_Atraso_SLA_(Complemento)': 'sla_delay_complement',
            '146013_Motivo_Orçamentos_Zerados/Fora_de_Margem': 'zero_out_of_margin_budget_reason',
            '146014_Motivo_Orçamentos_Zerados/Fora_de_Margem_(Complemento)': 'zero_out_of_margin_budget_complement',
            '74474_Número_da_Pasta': 'file_number',
            '158945_Pasta_do_Chamado': 'call_folder',
            '153475_Valor_Cobrado_do_Cliente_(Previsto)': 'estimated_client_charge',
            '93891_Valor_Cobrado_do_Cliente_(Real)': 'real_client_charge',
            '153478_Valor_de_Entrada': 'entry_value',
            '153594_Valor_Pagamento_Final': 'final_payment_value',
            '178948_Custo_SV': 'sv_cost',
            '92564_Detalhamento_Servico': 'service_detail'
        }

        df_tickets_detalhados.rename(columns=renomear_colunas, inplace=True)

        # Filtrar apenas as colunas que foram renomeadas
        colunas_para_manter = list(renomear_colunas.values())  # use values after rename to filter
        df_tickets_detalhados = df_tickets_detalhados[colunas_para_manter]

        # Concatenar ao DataFrame de resultados
        df_resultados = pd.concat([df_resultados, df_tickets_detalhados], ignore_index=True)
        df_resultados = df_resultados.where(pd.notnull(df_resultados), None)

    return df_resultados


def upsert(df, table_name, connection_params):
    try:
        # Inicia a conexão com o MySQL
        connection = mysql.connector.connect(**connection_params)
        cursor = connection.cursor()

        # Prepara a query de inserção com atualização em caso de duplicidade
        placeholders = ", ".join(["%s"] * len(df.columns))
        columns = ", ".join(df.columns)
        updates = ", ".join(f"{col}=VALUES({col})" for col in df.columns if col != 'id')  # Presumindo que 'id' é a chave primária
        query = (f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) "
                 f"ON DUPLICATE KEY UPDATE {updates}")

        # Prepara os dados para a inserção
        data = [tuple(x) for x in df.to_numpy()]

        # Executa a query para cada linha do dataframe
        cursor.executemany(query, data)
        connection.commit()  # Confirma as inserções/alterações na base

        print(f"{cursor.rowcount} linhas inseridas/atualizadas.")

    except Error as e:
        print(f"Erro ao inserir dados no MySQL: {e}")

    finally:
        # Fecha o cursor e a conexão
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão com MySQL fechada.")


if __name__ == "__main__":
    PROXY = None
    date_END = datetime.now()
    date_START = date_END - timedelta(days=7)
    START_DATE = date_START.strftime('%Y-%m-%d')
    END_DATE = date_END.strftime('%Y-%m-%d')
    
    tabela_mysql = os.getenv('tb_TICKETS')
    
    user = os.getenv('db_USER')
    password = os.getenv('db_PASSWORD')
    host = os.getenv('db_HOST')
    port = os.getenv('db_PORT')
    database = os.getenv('db_DATABASE')
    
    connection_params = {
        'user': user,
        'password': password,
        'host': host,
        'port': port,
        'database': database
    }

    df_final = processar_intervalo(PROXY, START_DATE, END_DATE)
    upsert(df_final, tabela_mysql, connection_params)
    
    
# Inclusão campo: 92564