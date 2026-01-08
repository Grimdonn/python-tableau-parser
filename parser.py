import xml.etree.ElementTree as ET
import re

def normalize_field_ref(field_ref):
    """
    Normalizes a Tableau field reference string by stripping out derivation
    and type tags (e.g., [none:FieldName:nk] -> [FieldName]).
    Handles both prefixed ([datasource].[field]) and unprefixed references.
    """
    if not field_ref:
        return field_ref
        
    parts = field_ref.split('].[')
    normalized_parts = []
    
    for part in parts:
        clean_part = part.strip('[]')
        
        # Check if the part has derivation tags (contains ':')
        if ':' in clean_part:
            # Format is usually [derivation:name:type]
            # We want the middle part
            sub_parts = clean_part.split(':')
            if len(sub_parts) >= 2:
                # If it's [none:Field:nk] or [sum:Field:qk]
                # The field name is index 1
                clean_part = sub_parts[1]
        
        normalized_parts.append(f"[{clean_part}]")
        
    return ".".join(normalized_parts)


def parse_twb(twb_path):
    """
    Parses the .twb XML file to extract data sources, sheets, dashboards, parameters,
    and field usage tracking.
    """
    try:
        tree = ET.parse(twb_path)
        root = tree.getroot()
        
        data = {
            "datasources": [],
            "worksheets": [],
            "dashboards": [],
            "parameters": []
        }

        # 1. Map Field Usage in Worksheets
        usage_map = {}

        for ws in root.findall(".//worksheet"):
            ws_name = ws.get('name')
            if not ws_name: continue
            
            data["worksheets"].append(ws_name)
            
            # Roles we track: Rows, Columns, Filters
            rows = ws.find(".//rows")
            cols = ws.find(".//column") # Note: XML uses <column> tag inside <table> for columns
            
            if rows is not None and rows.text:
                for field in rows.text.split('/'):
                    field = field.strip('()')
                    if field:
                        norm_field = normalize_field_ref(field)
                        usage_map.setdefault(norm_field, {}).setdefault(ws_name, []).append("Row")
            
            if cols is not None and cols.text:
                for field in cols.text.split('/'):
                    field = field.strip('()')
                    if field:
                        norm_field = normalize_field_ref(field)
                        usage_map.setdefault(norm_field, {}).setdefault(ws_name, []).append("Column")

            # Filters and Slices
            for filter_node in ws.findall(".//filter"):
                col_name = filter_node.get('column')
                if col_name:
                    norm_field = normalize_field_ref(col_name)
                    usage_map.setdefault(norm_field, {}).setdefault(ws_name, []).append("Filter")
            
            for slice_node in ws.findall(".//slices/column"):
                if slice_node.text:
                    norm_field = normalize_field_ref(slice_node.text)
                    usage_map.setdefault(norm_field, {}).setdefault(ws_name, []).append("Filter")

        # 2. Extract Dashboards
        for db in root.findall(".//dashboard"):
            name = db.get('name')
            if name:
                data["dashboards"].append(name)

        # 3. Extract Data Sources and Parameters
        for ds in root.findall(".//datasource"):
            caption = ds.get('caption') or ds.get('name')
            
            # Parameters Handling
            if caption == 'Parameters' or (ds.get('name') and ds.get('name').startswith('Parameters')):
                for col in ds.findall(".//column"):
                    p_name = col.get('caption') or col.get('name')
                    if p_name:
                        data["parameters"].append(p_name.strip('[]'))
                continue

            if not caption:
                continue
            
            ds_info = {
                "name": caption, 
                "tech_name": ds.get('name'), 
                "connections": [], 
                "queries": [], 
                "fields": []
            }
            
            # Connections
            for conn in ds.findall(".//connection"):
                conn_class = conn.get('class')
                if conn_class and conn_class != 'federated':
                    ds_info["connections"].append({
                        "class": conn_class,
                        "server": conn.get('server'),
                        "dbname": conn.get('dbname')
                    })

            # Custom SQL
            found_queries = []
            # Note: User explicitly changed .false to .true here to see metadata relations
            for rel in ds.findall(".//*[@type='text']"):
                tag = rel.tag
                if 'ObjectModelEncapsulateLegacy' in tag:
                    if '.true' not in tag:
                        continue
                
                if rel.tag.endswith('relation') or 'relation' in rel.tag:
                    sql = rel.text
                    if sql and sql.strip():
                        query_text = sql.strip()
                        if query_text not in found_queries:
                            found_queries.append(query_text)
            
            ds_info["queries"] = found_queries
            
            # Field Discovery from Metadata Records
            # dictionary keyed by tech_name (local-name)
            fields_by_tech = {}
            
            for mr in ds.findall(".//metadata-records/metadata-record[@class='column']"):
                local_name = mr.findtext('local-name')
                if not local_name: continue
                
                fields_by_tech[local_name] = {
                    "tech_name": local_name,
                    "name": mr.findtext('remote-name') or local_name.strip('[]'),
                    "role": "",
                    "datatype": mr.findtext('local-type') or "",
                    "formula": "",
                    "usage": ""
                }

            # Field Enrichment from Column tags (UI captures, Roles, Calcs)
            for col in ds.findall(".//column"):
                name_attr = col.get('name')
                if not name_attr: continue
                
                # If we don't have it from metadata, it might be a calculated field or UI parameter
                if name_attr not in fields_by_tech:
                    fields_by_tech[name_attr] = {
                        "tech_name": name_attr,
                        "name": col.get('caption') or name_attr.strip('[]'),
                        "role": "",
                        "datatype": "",
                        "formula": "",
                        "usage": ""
                    }
                
                field_obj = fields_by_tech[name_attr]
                
                # Prefer caption if exists
                if col.get('caption'):
                    field_obj["name"] = col.get('caption')
                
                # Fill in Role/DataType if missing or if column tag provides specialized role
                if col.get('role'):
                    field_obj["role"] = col.get('role').capitalize()
                if col.get('datatype'):
                    field_obj["datatype"] = col.get('datatype').capitalize()
                
                # Check for calculation
                calc = col.find("calculation")
                if calc is not None:
                    field_obj["formula"] = calc.get('formula') or ""
                    field_obj["role"] = "Calculation"

            # Finalize Usage Mapping and Build Field List
            ds_tech_prefix = ds_info["tech_name"] if ds_info["tech_name"] else ""
            
            for tech_name, field_obj in fields_by_tech.items():
                # Usage Info
                tech_names_to_check = [tech_name]
                if ds_tech_prefix and not tech_name.startswith(f"[{ds_tech_prefix}]"):
                    tech_names_to_check.append(f"[{ds_tech_prefix}].{tech_name}")

                usage_info = {}
                for tn in tech_names_to_check:
                    if tn in usage_map:
                        for ws_name, roles in usage_map[tn].items():
                            usage_info.setdefault(ws_name, set()).update(roles)

                usage_str_parts = []
                for ws_name, roles in usage_info.items():
                    role_str = "/".join(sorted(list(roles)))
                    usage_str_parts.append(f"{ws_name} ({role_str})")
                
                field_obj["usage"] = ", ".join(usage_str_parts) if usage_str_parts else "Not Used"
                
                # Add to ds_info list
                ds_info["fields"].append(field_obj)
            
            if ds_info["connections"] or ds_info["queries"] or ds_info["fields"]:
                data["datasources"].append(ds_info)
        #print(data)
        return data
    except Exception as e:
        print(f"Error parsing .twb file '{twb_path}': {e}")
        return None
