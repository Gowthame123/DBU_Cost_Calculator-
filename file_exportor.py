import io
import pandas as pd
import xlsxwriter
import streamlit as st


def generate_consolidated_excel_export(calculated_dbx_data, s3_calc_method, s3_direct_config, s3_table_based_config, sql_warehouses_config):
    """
    Generates a consolidated Excel file with multiple sheets for different cost categories.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:

        # 1. Databricks Jobs Sheet (All Tiers Combined)
        all_dbx_dfs = []
        for tier, data in calculated_dbx_data.items():
            df_to_export = data['df'].copy()

            # Add a 'Tier' column to identify the original tier for each job
            # if 'Spot' not in df_to_export.columns:
            #     df_to_export['Spot'] = False
            df_to_export['Tier'] = tier

            all_dbx_dfs.append(df_to_export)

        if all_dbx_dfs:
            combined_dbx_df = pd.concat(all_dbx_dfs, ignore_index=True)

            # Rename columns for clarity in Excel
            combined_dbx_df = combined_dbx_df.rename(columns={
                'Job Name': 'Name',
                #'Job_Number': 'Job No',
                'Runtime (hrs)': 'Runtime Hours',
                'Runs/Month': 'Runs per Month',
                'Compute type': 'Compute Type',
                'Instance Type': 'Instance',
                'Nodes': 'worker_Nodes',
                'Photon': 'Photon Enabled',
                'Spot': 'Spot Instance',
                'DBU': 'Calculated DBU', # Assuming DBU is DBU cost
                #'EC2': 'Calculated EC2 Cost ($)',
                'DBX': 'Calculated DBX Cost ($)',
                'EC2': 'Calculated EC2 Cost ($)'
            })

            # Define the final order of columns for export.
            # Only include columns that are actually present in your DataFrame.
            ordered_cols_dbx = [
                'Tier', 'Name', 'Runtime Hours', 'Runs per Month', 'Compute Type',
                'Instance', 'worker_Nodes', 'Photon Enabled', 'Spot Instance',
                'Calculated DBU', 'Calculated DBX Cost ($)', 'Calculated EC2 Cost ($)'
            ]

            # Reorder the DataFrame, dropping any columns not in the final list.
            combined_dbx_df = combined_dbx_df[ordered_cols_dbx]

                        # This prevents the KeyError.
            present_cols = [col for col in ordered_cols_dbx if col in combined_dbx_df.columns]
            combined_dbx_df = combined_dbx_df[present_cols]

            combined_dbx_df.to_excel(writer, sheet_name="Databricks_Jobs", index=False)
        else:
            # Create an empty DataFrame with expected columns if no data
            empty_dbx_df = pd.DataFrame(columns=[
                'Tier', 'Name', 'Runtime Hours', 'Runs per Month', 'Compute Type',
                'Instance', 'worker_Nodes', 'Photon Enabled', 'Spot Instance',
                'Calculated DBU', 'Calculated DBX Cost ($)', 'Calculated EC2 Cost ($)'
            ])
            empty_dbx_df.to_excel(writer, sheet_name="Databricks_Jobs", index=False)


        # 2. S3 Storage Sheets (based on active method)
        if s3_calc_method == "Direct Storage":
            direct_data = []
            for zone, config in s3_direct_config.items():
                direct_data.append({
                    "Zone": zone,
                    "Storage Class": config["class"],
                    "Storage Amount": config["amount"],
                    "Unit": config["unit"],
                    "Monthly Growth %": config["monthly_growth_percent"]
                })
            if direct_data:
                df_direct = pd.DataFrame(direct_data)
                df_direct.to_excel(writer, sheet_name='S3_Direct_Storage', index=False)
            else:
                empty_s3_direct_df = pd.DataFrame(columns=["Zone", "Storage Class", "Storage Amount", "Unit", "Monthly Growth %"])
                empty_s3_direct_df.to_excel(writer, sheet_name='S3_Direct_Storage', index=False)

        else: # Table-Based
            consolidated_table_data_for_export = []
            for zone, list_of_table_configs in s3_table_based_config.items():
                if not isinstance(list_of_table_configs, list):
                    list_of_table_configs = [list_of_table_configs] if isinstance(list_of_table_configs, dict) else []

                for table_config in list_of_table_configs:
                    if isinstance(table_config, dict):
                        row = {
                            "Zone": zone,
                            "Table Name": table_config.get("Table Name", ""),
                            "Records": table_config.get("Records", 0),
                            "Columns": table_config.get("Columns", 0)
                        }
                        consolidated_table_data_for_export.append(row)

            if consolidated_table_data_for_export:
                df_table = pd.DataFrame(consolidated_table_data_for_export)
                ordered_cols_s3_table = ["Zone", "Table Name", "Records", "Columns"]
                df_table = df_table[ordered_cols_s3_table]
                df_table.to_excel(writer, sheet_name='S3_Table_Based_Storage', index=False)
            else:
                empty_s3_table_df = pd.DataFrame(columns=["Zone", "Table Name", "Records", "Columns"])
                empty_s3_table_df.to_excel(writer, sheet_name='S3_Table_Based_Storage', index=False)

        # 3. SQL Warehouses Sheet
        global_data = st.session_state.get('global_data', {})
        sql_flat_rate_card = global_data.get('SQL_FLAT_RATE_CARD', {})
        sql_rates_by_type_and_instance = global_data.get('SQL_RATES_BY_TYPE_AND_INSTANCE', {})
        sql_flat_instance_list = global_data.get('SQL_FLAT_INSTANCE_LIST', {})
        if sql_warehouses_config:
            warehouse_data = []
            for wh in sql_warehouses_config:
                # FIX 2: Add a check to prevent AttributeError
                if wh["size"] and " - " in wh["size"]:
                    # Get the instance name (e.g., '2X-Small') from the size string
                    warehouse_type = wh.get("type")
                    size_string = wh.get("size")

                    instance_name = sql_flat_instance_list.get(size_string)
                    
                    # Use the nested dictionary for a reliable lookup
                    rates = sql_rates_by_type_and_instance.get(warehouse_type, {}).get(instance_name, {})
                    
                    dbt_per_hr = rates.get("DBU/hour", 0)
                    hourly_rate = rates.get("Rate/hour", 0)
                    nodes = wh.get("SQL_nodes", 1)
                    
                    warehouse_data.append({
                        "Name": wh["name"],
                        "Type": wh["type"],
                        "Size": instance_name,
                        "DBUs per Hour": dbt_per_hr,
                        "Hourly Rate ($)": hourly_rate,
                        "Nodes": nodes,
                        "Hours per Day": wh["hours_per_day"],
                        "Days per Month": wh["days_per_month"],
                        "Monthly Cost ($)": hourly_rate * wh["hours_per_day"] * wh["days_per_month"] * nodes,
                    })
                else:
                    # Handle cases with no valid size data
                    warehouse_data.append({
                        "Name": wh["name"],
                        "Type": wh["type"],
                        "Size": "N/A",
                        "DBUs per Hour": 0,
                        "Hourly Rate ($)": 0,
                        "Nodes": wh["SQL_nodes"],
                        "Hours per Day": wh["hours_per_day"],
                        "Days per Month": wh["days_per_month"],
                        "Monthly Cost ($)": 0,
                    })

            df_sql = pd.DataFrame(warehouse_data)
            ordered_cols_sql = [
                "Name", "Type", "Size", "DBUs per Hour", "Hourly Rate ($)","Nodes",
                "Hours per Day", "Days per Month", "Monthly Cost ($)"
            ]
            df_sql = df_sql[ordered_cols_sql]
            df_sql.to_excel(writer, sheet_name='SQL_Warehouses', index=False)
        else:
            empty_sql_df = pd.DataFrame(columns=[
                "Name", "Type", "Size", "DBUs per Hour", "Hourly Rate ($)","Nodes",
                "Hours per Day", "Days per Month", "Monthly Cost ($)"
            ])
            empty_sql_df.to_excel(writer, sheet_name='SQL_Warehouses', index=False)

    output.seek(0)
    return output.getvalue()