import requests
import logging
import traceback
import datetime

from flask import Flask, request, Response
from pydantic_settings import BaseSettings, SettingsConfigDict
from simple_baserow_api import BaserowApi   # Olis Baserow API wrapper
from simple_sams_api import SAMSapi, filter_phenopacket_by_onset, extract_HPO_terms_from_phenopacket


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Load configuration from environment variables
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file="webhook_listener.env")
    baserow_url: str
    baserow_token: str

    existing_index_col: str

    # Genotips
    genotips_baserow_url: str
    genotips_neuro_baserow_token: str
    genotips_neuro_faelle_table_id: str
    genotips_faelle_initial_HPO_field: str

    sams_user_neurologie: str
    sams_password_neurologie: str

    # ----- Modellvorhaben -------
    mv_faelle_table_id: int
    mv_faelle_table_index_col: str

    mv_findings_table_id: int
    mv_findings_table_index_col: str

    mv_laboranalysen_table_id: int
    mv_laboranalysen_table_index_col: str
    
    # 1. Laborauftrag
    mv_faelle_laborauftrag_webhook_trigger: str
    mv_laborkommentare_table_id: int
    mv_laborkommentare_table_index_col: str

    # 2. Bearbeitungsstatus
    mv_faelle_bearbeitung_webhook_trigger: str
    mv_faelle_status_datum_col: str

    # 2.2 Set Dauer FK1 to 0:10
    mv_faelle_datum_fk1: str
    mv_faelle_dauer_fk1: str

    # 3. Laboranalyse
    mv_findings_laboranalyse_webhook_trigger: str
    mv_laboranalysen_webhook_trigger: str

    # 4. Reanalyse anfordern
    mv_faelle_reanalyse_webhook_trigger: str

    # ---- DB Molekulargenetik -----
    db_molekulargenetik_faelle_table_id: int

    db_molekulargenetik_faelle_table_index_col: str
    db_molekulargenetik_faelle_synchro_status_col: str

    db_molekulargenetik_faelle_table_last_user_changed_col: str
    db_molekulargenetik_faelle_table_probeneingangs_control_date_col: str
    db_molekulargenetik_faelle_table_probeneingangs_control_by_col: str
    db_molekulargenetik_faelle_table_dna_extracted_date_col: str
    db_molekulargenetik_faelle_table_dna_extracted_by_col: str
    db_molekulargenetik_faelle_table_dna_measurement_col: str
    db_molekulargenetik_faelle_table_dna_measurement_by_col: str
    db_molekulargenetik_faelle_table_archive_extraction_date_col: str
    db_molekulargenetik_faelle_table_archive_extraction_by_col: str
    db_molekulargenetik_faelle_table_archive_restorage_date_col: str
    db_molekulargenetik_faelle_table_archive_restorage_by_col: str
    db_molekulargenetik_faelle_table_lib_prep_date_col: str
    db_molekulargenetik_faelle_table_lib_prep_by_col: str
    db_molekulargenetik_faelle_table_status_datum_col: str
    db_molekulargenetik_faelle_table_library_destroyed_date_col: str
    db_molekulargenetik_faelle_table_library_destroyed_by_col: str


MEDGEN_LABORAUFTRAG_DICT = {
    "Kommentar": "Kommentar Einschluss"
}

app = Flask(__name__)
settings = Settings()
baserow_api = BaserowApi(database_url=settings.baserow_url, token=settings.baserow_token)


def create_HTML_response(header:str, paragraphs:list[str], 
                         additional_html:str="", failure:bool=False,
                         status_code:int=200):
    """Helper function to create a standardized HTML response for the webhook listener."""
    paragraphs_html = "".join([f"<p>{paragraph}</p>" for paragraph in paragraphs])
    
    if failure:
        footer = """<p>If issue persists: Contact support at <a href="mailto:oliver.kuechler@charite.de">Oliver Kuechler</a> for assistance.</p>"""
    else:
        footer = "<p>You can close this window now.</p>"

    header_colored = f"<h1 style='color:red'>{header}</h1>" if failure else f"<h1 style='color:green'>{header}</h1>"
    response = Response(f"""        
        <html>
            <body>
                {header_colored}
                {paragraphs_html}
                {additional_html}
                {footer}
            </body>
        </html>
    """, status=status_code, mimetype='text/html')
    return response


def _split_sync_messages(messages: list[str]) -> tuple[list[str], list[str]]:
    """Split synchronize_data messages into transfer warnings vs dry-run notices."""
    transfer_warnings = []
    dry_run_notices = []
    for msg in messages:
        if msg.startswith("dry_run:"):
            dry_run_notices.append(msg)
        else:
            transfer_warnings.append(msg)
    return transfer_warnings, dry_run_notices


def _warnings_as_html_list(warnings_list: list[str]) -> str:
    """Render transfer warnings as an HTML unordered list."""
    if not warnings_list:
        return ""
    items = "".join(f"<li>{msg}</li>" for msg in warnings_list)
    return f"<ul>{items}</ul>"


def sanitize_row_data(row_dict: dict, include_fields: list[str] = None, 
                      extract_values_fields: set = None, extract_all_values_fields: bool = False) -> dict:
    """ Sanitize row data before adding to Baserow
        1. Keep only target fields if specified
        2. Remove nested dict values

    Args:
        row_dict (dict): The row data to be sanitized
        include_fields (list[str], optional):  List of fields to include in the sanitized row. If None, all fields are included. Defaults to None.
        extract_values_fields (set, optional): Extract values (instead of IDs) from nested dicts or lists. Defaults to None.
        extract_all_values_fields (bool, optional): Extract values for all fields that are lists. Defaults to False.
    Returns:
        dict: The sanitized row data
    """
    # Keep only target fields if specified
    sanitized_dict = {key: value for key, value in row_dict.items() if not include_fields or key in include_fields}

    # Process nested dict and list values
    for key, value in sanitized_dict.items():
        if isinstance(value, dict) and "value" in value:
            sanitized_dict[key] = value["value"]
        elif isinstance(value, list):
            if extract_all_values_fields or (extract_values_fields and key in extract_values_fields):
                sanitized_dict[key] = [entry["value"] if isinstance(entry, dict) and "value" in entry else entry for entry in value]
            else:
                sanitized_dict[key] = [entry["id"] if isinstance(entry, dict) and "id" in entry else entry for entry in value]
    return sanitized_dict


def get_row_id_for_matching_field(table_id: int, field_name: str, 
                                  field_value: str) -> int:
    """ Get the row ID for a matching row given a specific field name and value.
    Args:
        table_id (int):     The table ID of the target table
        field_name (str):   The field/col name where the value should be matched
        field_value (str):  The field value to match with

    Returns:
        int: The row ID if a matching row is found, None otherwise
    """    
    res_data = baserow_api.get_data(table_id, include=[field_name])
    for row_id, row_values in res_data.items():
        if row_values.get(field_name) == field_value:
            return row_id
    
    logger.warning(f"Failed to get row ID for matching field {field_name} with value {field_value} in table {table_id}. Will retry once.")
    return None


def create_or_update_item(item: dict, target_table_id: int,
                        source_index_col: str = None, target_index_col: str = None,
                        include_fields: list[str] = None) -> int:
    """Check if a row with the same index value already exists in the target table. 
    If yes, update the existing row (keep the same row ID, update the values);
    if not, create a new row (set row ID to None, so that Baserow creates a new row with a new ID).

    Args:
        item (dict):            The item to be copied (row data)
        target_table_id (int):  The ID of the target table
        source_index_col (str): The index column in source table -> deciding whether to update or create a new row
        target_index_col (str): The index column in target table -> deciding whether to update or create a new row
        include_fields (Optional[list[str]], optional): List of fields to include into the target table. Defaults to None.
    Returns:
        int: The 
    """
    
    # Load already existing data from target table
    table_data = baserow_api.get_data(target_table_id, include=include_fields)
    item["id"] = None   # default: create a new row

    for target_row_id, target_row_values in table_data.items():
        if source_index_col and target_index_col:
            try:
                # Only update if row with the same index value already exists in the target table; 
                # otherwise create a new row
                if item[source_index_col] == target_row_values[target_index_col]:
                    item["id"] = target_row_id    # set the row ID
            except KeyError as e:
                logger.error(f"KeyError: {e} for item: {item} - value: {target_row_values}")
                continue
    
    # Copy original index value to index column of target table
    if source_index_col and target_index_col:
        item[target_index_col] = item[source_index_col]

    return item


def copy_sourceData_to_targetTable(item: dict, source_index_col: str,
                                   target_table_id: int, target_index_col: str, 
                                   include_fields: list[str] = None,
                                   ) -> tuple:
    """Copy data from source table to target table 

    Args:
        item (dict): The item to be copied (row data)
        source_index_col (str): The index column -> to check for duplicates / whether to update or create a new row
        target_table_id (int): The target table ID
        target_index_col (str): The index column -> to check for duplicates / whether to update or create a new row
        include_fields (Optional[list[str]], optional): List of fields to include in the target table. Defaults to None.

    Returns:
        tuple: A tuple containing the list of created/updated row IDs and any errors that occurred during the operation
    """
    # Create updated item & keep only writable fields
    updated_item = create_or_update_item(item, target_table_id, source_index_col, 
                                      target_index_col, include_fields=include_fields)
    writable_fields =[entry["name"] for entry in baserow_api.get_writable_fields(target_table_id)]
    updated_item = {key: value for key, value in updated_item.items() 
                    if key in writable_fields + ["id"] and 
                    (include_fields is None or key in include_fields)}
    new_data_rows = [updated_item]
    logger.info(f"New data rows: {new_data_rows}")

    try:
        row_ids, errors = baserow_api.add_data_batch(target_table_id, new_data_rows, fail_on_error=True)
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to add data batch to Baserow: {e}")
        logger.error(f"Response content: {e.response.content}")
        raise e
    
    return row_ids, errors


# WARNING: Still used by DB Molekulargenetik tables!
@app.route('/webhook', methods=['POST'])
def webhook():
    # Check if request is JSON
    # logger.info(f"Received webhook request: {request}")
    if request.content_type == 'application/json':
        headers = request.headers
        data = request.json
        logger.info(f"headers: {headers}")
    else:
        logger.warning(f"Unsupported Content-Type: {request.content_type}")
        return Response("Unsupported Media Type", status=415, mimetype='text/plain')

    # Register event type & process data
    source_items = []
    event_type = data.get('event_type')
    table_id = data.get('table_id')
    if event_type and table_id:
        logger.info(f"Received event type: {event_type} for table_id: {data.get('table_id')}")
        if event_type == 'rows.updated' or event_type == 'rows.created':
            logger.info(f"Rows updated or created event received")
            source_items = data.get('items')

            try:
                process_webhook(table_id, source_items, headers)
                logger.info(f"Processed webhook successfully")
                return Response("Success!", status=200, mimetype='text/plain')
            except Exception as e:
                logger.error(f"Error processing webhook: {e}")
                logger.error(f"Error details: {e.__traceback__}")
                # return Response("Internal Server Error", status=500, mimetype='text/plain')
                return Response("Internal Server Error", status=200, mimetype='text/plain')

        elif event_type == 'rows.deleted':
            logger.info(f"Rows deleted event received. I will not process this event")
            return Response("No action taken for deleted rows", status=200, mimetype='text/plain')
        else:
            logger.warning(f"Unsupported event type: {event_type}")
            # return Response("Bad Request", status=400, mimetype='text/plain')
            return Response("Bad Request", status=200, mimetype='text/plain')
    else:
        logger.warning(f"Event type not found in request")
        # return Response("Bad Request", status=400, mimetype='text/plain')
        return Response("Bad Request", status=200, mimetype='text/plain')


@app.route('/do-medgen-laborauftrag/<tableID>/<rowID>', methods=['GET'])
def process_medgen_laborauftrag(tableID, rowID):
    """Transfer an MV Faelle row into DB Molekulargenetik Faelle.

    Uses :meth:`BaserowApi.synchronize_data` to copy all compatible fields
    (with warnings for values that cannot be transferred). The special
    ``existierender Index`` link is resolved separately after the sync.

    Query params:
        dry_run: If ``1``/``true``, preview the transfer without writing.
    """
    logger.info("Webhook trigger 'Patho - Laborauftrag auflösen' set. Will process this item.")

    source_table_id = settings.mv_faelle_table_id
    source_index_col = settings.mv_faelle_table_index_col
    target_table_id = settings.db_molekulargenetik_faelle_table_id
    target_index_col = settings.db_molekulargenetik_faelle_table_index_col
    exist_index_col = settings.existing_index_col

    tableID = int(tableID)
    rowID = int(rowID)
    dry_run = request.args.get("dry_run", "").lower() in ("1", "true", "yes")

    try:
        item = baserow_api.get_entry(
            table_id=tableID, row_id=rowID, use_linked_row_ids=True
        )
        logger.info(f"Retrieved item from Baserow: {item}")

        # Handle "existierender Index": resolve to a target-table row ID.
        existing_index_entry = item.get(exist_index_col)
        matching_index_entry_id = None
        item_is_index = False
        if isinstance(existing_index_entry, list) and len(existing_index_entry) > 0:
            existing_index_row_id = int(existing_index_entry[0])
            item_is_index = existing_index_row_id == rowID
            existing_index_individuum_id = baserow_api.get_entry(
                table_id=source_table_id,
                row_id=existing_index_row_id,
            ).get(source_index_col)
            logger.info(
                "Resolved existierender Index source row %s → Individuum ID %s "
                "(item_is_index=%s)",
                existing_index_row_id,
                existing_index_individuum_id,
                item_is_index,
            )

            if not item_is_index:
                matches = baserow_api.find_entries(
                    target_table_id,
                    target_index_col,
                    existing_index_individuum_id,
                )
                if not matches:
                    raise ValueError(
                        f"Kein passendes Index-Individuum in Zieltabelle gefunden "
                        f"für existierenden Index: {existing_index_individuum_id} - "
                        f"Bitte zuerst den Index übertragen, damit der existierende "
                        f"Index in der DB Molekulargenetik Faelle Tabelle gefunden "
                        f"werden kann."
                    )
                matching_index_entry_id = next(iter(matches))
                if len(matches) > 1:
                    logger.warning(
                        "Multiple target rows match existierender Index Individuum "
                        "ID %s; using first row %s",
                        existing_index_individuum_id,
                        matching_index_entry_id,
                    )
        else:
            raise ValueError("'existierender Index' not found in item")

        field_mapping = dict(MEDGEN_LABORAUFTRAG_DICT)
        if source_index_col != target_index_col:
            field_mapping[source_index_col] = target_index_col

        # Transfer all compatible fields; existierender Index is set afterwards.
        target_row_id, messages = baserow_api.synchronize_data(
            source_table_id=tableID,
            source_row_id=rowID,
            target_table_id=target_table_id,
            identifier_column=source_index_col,
            field_mapping=field_mapping,
            exclude_fields=[exist_index_col],
            fail_on_error=False,
            dry_run=dry_run,
        )

        for msg in messages:
            logger.warning("Sync notice/skip: %s", msg)

        transfer_warnings, dry_run_notices = _split_sync_messages(messages)
        # Plain-text summary for Baserow status field (no HTML).
        status_warning_summary = ""
        if transfer_warnings:
            status_warning_summary = (
                f" Warnings ({len(transfer_warnings)}): "
                + "; ".join(transfer_warnings)
            )

        table_link_html = (
            f"<p><a href=\"{settings.baserow_url}/database/345/table/"
            f"{target_table_id}\">DB Molekulargenetik Faelle Tabelle</a></p>"
        )
        warnings_list_html = _warnings_as_html_list(transfer_warnings)

        if dry_run:
            action = (
                f"would update target row {target_row_id}"
                if target_row_id is not None
                else "would create a new target row"
            )
            logger.info("Dry-run complete: %s", action)
            paragraphs = [
                "Keine Daten wurden geschrieben (dry_run=1).",
                f"Geplante Aktion: {action}.",
                f"Existierender-Index Ziel-ID (nach Sync): "
                f"{matching_index_entry_id if not item_is_index else '(self / nach Create)'}.",
            ]
            # Dry-run notices (would create/update, payload fields) as own paragraphs.
            for notice in dry_run_notices:
                paragraphs.append(notice)
            if transfer_warnings:
                paragraphs.append(
                    f"Transfer-Warnungen ({len(transfer_warnings)}):"
                )
            else:
                paragraphs.append("Keine Transfer-Warnungen.")

            return create_HTML_response(
                header="Patho - Laborauftrag auflösen: Dry-Run",
                paragraphs=paragraphs,
                additional_html=warnings_list_html + table_link_html,
            )

        current_entry_id = target_row_id
        if not matching_index_entry_id and item_is_index:
            matching_index_entry_id = current_entry_id

        logger.info(
            "Setting existierender Index on target row %s → %s",
            current_entry_id,
            matching_index_entry_id,
        )
        baserow_api.add_data(
            table_id=target_table_id,
            row_id=current_entry_id,
            data={exist_index_col: matching_index_entry_id},
        )

        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        success_message = f"Successful transfer: {current_date}"
        if status_warning_summary:
            success_message = success_message + status_warning_summary
        baserow_api.add_data(
            table_id=source_table_id,
            row_id=rowID,
            data={settings.db_molekulargenetik_faelle_synchro_status_col: success_message},
        )
        logger.info("Updated synchronization status to: %s", success_message)

        paragraphs = [
            "Die Daten wurden erfolgreich in die DB Molekulargenetik Faelle "
            "Tabelle übertragen.",
            f"Ziel-Zeile: {current_entry_id}.",
            f"Der Synchronisierungsstatus wurde aktualisiert: "
            f"Successful transfer: {current_date}",
        ]
        if transfer_warnings:
            paragraphs.append(
                f"Einige Felder konnten nicht vollständig übertragen werden "
                f"({len(transfer_warnings)} Warnungen):"
            )
        else:
            paragraphs.append("Alle übertragbaren Felder wurden synchronisiert.")

        return create_HTML_response(
            header="Patho - Laborauftrag auflösen: Erfolgreich",
            paragraphs=paragraphs,
            additional_html=(
                warnings_list_html
                + f"<p><a href=\"{settings.baserow_url}/database/345/table/"
                f"{target_table_id}\">Hier geht's zur DB Molekulargenetik "
                f"Faelle Tabelle</a></p>"
            ),
        )

    except Exception as e:
        logger.error(f"Error during DB-Molekulargenetik-Synchro processing: {e}")
        logger.error(f"Error details: {traceback.format_exc()}")

        error_message = f"Error: {str(e)}"
        if not dry_run:
            try:
                baserow_api.add_data(
                    table_id=source_table_id,
                    row_id=rowID,
                    data={
                        settings.db_molekulargenetik_faelle_synchro_status_col: error_message
                    },
                )
                logger.info(
                    "Updated synchronization status with error: %s", error_message
                )
            except Exception as status_error:
                logger.error(f"Failed to update synchronization status: {status_error}")

        return create_HTML_response(
            header="Patho - Laborauftrag auflösen: Fehler",
            paragraphs=[
                f"Es gab einen Fehler bei der Verarbeitung des Webhooks: {str(e)}",
                f"Status: {error_message}",
            ],
            failure=True,
        )


def process_webhook(table_id: int, source_items: dict, headers: dict) -> None:

    for item in source_items:
        # check if id != 0
        if item["id"] == 0:
            logger.warning("Item ID is 0. Will not process this item")
            continue

        # logger.info(f"Processing item: {item}")

        # ======= MV FAELLE =======
        if table_id == settings.mv_faelle_table_id:
            raise NotImplementedError("Processing for MV Faelle table is not implemented anymore...")

        # ======= MV Findings =======
        elif table_id == settings.mv_findings_table_id:
            # 3. Laboranalyse
            raise NotImplementedError("Processing for MV Findings table is not implemented anymore...")

        # ======= MV Laboranalysen =======
        elif table_id == settings.mv_laboranalysen_table_id:
            # 4. Set Proben ID
            raise NotImplementedError("Processing for MV Laboranalysen table is not implemented anymore...")

        # ======= DB Molekulargenetik Faelle =======
        elif table_id in [settings.db_molekulargenetik_faelle_table_id]:
            if "Trigger" in headers:
                logger.info(f"Trigger found in headers: {headers['Trigger']}")
                current_time_iso = datetime.datetime.now().isoformat()
                active_user = [item.get(settings.db_molekulargenetik_faelle_table_last_user_changed_col)]
                logger.info(f"Active user from row entry: {active_user}")

                if headers["Trigger"] == "Bearbeitungsstatus-Update":
                    logger.info(f"Webhook trigger 'Bearbeitungsstatus-Update' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                                            data={f"{settings.db_molekulargenetik_faelle_table_status_datum_col}": current_time_iso})

                # Set Probeneingangsprüfung-Erfolg date
                elif headers["Trigger"] == "Probeneingangspruefung-Erfolg":
                    logger.info(f"Webhook trigger 'Probeneingangspruefung-Erfolg' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                                            data={f"{settings.db_molekulargenetik_faelle_table_probeneingangs_control_date_col}": current_time_iso,
                                                  f"{settings.db_molekulargenetik_faelle_table_probeneingangs_control_by_col}": active_user})
                
                elif headers["Trigger"] == "DNA-Extraktion-Erfolg":
                    logger.info(f"Webhook trigger 'DNA-Extraktion-Erfolg' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                        data={f"{settings.db_molekulargenetik_faelle_table_dna_extracted_date_col}": current_time_iso,
                              f"{settings.db_molekulargenetik_faelle_table_dna_extracted_by_col}": active_user})
                    
                elif headers["Trigger"] == "DNA-Messung-Erfolg":
                    logger.info(f"Webhook trigger 'DNA-Messung-Erfolg' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                        data={f"{settings.db_molekulargenetik_faelle_table_dna_measurement_col}": current_time_iso,
                              f"{settings.db_molekulargenetik_faelle_table_dna_measurement_by_col}": active_user})
                
                elif headers["Trigger"] == "Archiv-Materialentnahme":
                    logger.info(f"Webhook trigger 'Archiv-Materialentnahme' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                        data={f"{settings.db_molekulargenetik_faelle_table_archive_extraction_date_col}": current_time_iso,
                              f"{settings.db_molekulargenetik_faelle_table_archive_extraction_by_col}": active_user})
                elif headers["Trigger"] == "Archiv-Materialruecklagerung":
                    logger.info(f"Webhook trigger 'Archiv-Materialruecklagerung' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                        data={f"{settings.db_molekulargenetik_faelle_table_archive_restorage_date_col}": current_time_iso,
                              f"{settings.db_molekulargenetik_faelle_table_archive_restorage_by_col}": active_user})
                elif headers["Trigger"] == "Library-Prep":
                    logger.info(f"Webhook trigger 'Library-Prep' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                        data={f"{settings.db_molekulargenetik_faelle_table_lib_prep_date_col}": current_time_iso,
                              f"{settings.db_molekulargenetik_faelle_table_lib_prep_by_col}": active_user})
                elif headers["Trigger"] == "Library-Vernichtung":
                    logger.info(f"Webhook trigger 'Library-Vernichtung' set. Will process this item.")
                    baserow_api.add_data(table_id=table_id, row_id=item["id"],
                        data={f"{settings.db_molekulargenetik_faelle_table_library_destroyed_date_col}": current_time_iso,
                              f"{settings.db_molekulargenetik_faelle_table_library_destroyed_by_col}": active_user})
                
                else:
                    logger.info(f"Webhook trigger not set. Will not process this item: {item}")
                    continue
                
        else:
            logger.info(f"Webhook trigger not set. Will not process this item")
            continue


@app.route('/', methods=['GET'])
def health():
    return Response("OK", status=200, mimetype='text/plain')


@app.route('/test-trigger', methods=['GET'])
def test_trigger():
    logger.info("Test trigger endpoint called")

    return jsonify({"message": "Test trigger received successfully!"}), 200



@app.route('/sams-synchro-for-genotips/<tableID>/<rowID>/<patientID>', methods=['GET'])
def sams_synchro(tableID, rowID, patientID):
    """Endpoint to synchronize the initial phenopacket from SAMS to the corresponding Baserow entry in Genotips."""
    logger.info("Sams synchro endpoint called with tableID: %s, rowID: %s and patientID: %s", \
                tableID, rowID, patientID)

    if tableID == settings.genotips_neuro_faelle_table_id:
        sams_user = settings.sams_user_neurologie
        sams_password = settings.sams_password_neurologie

        genotips_api = BaserowApi(database_url=settings.genotips_baserow_url, 
                                  token=settings.genotips_neuro_baserow_token)
    else:
        logger.error("Invalid tableID provided: %s", tableID)
        return create_HTML_response(
            header="Error in SAMS Synchronization",
            paragraphs=[f"Invalid tableID provided: {tableID}. Must be either {settings.genotips_fbrek_faelle_table_id} for FBREK or {settings.genotips_neuro_faelle_table_id} for NEURO.",
                        "Please provide a valid tableID."],
            status_code=400,
            failure=True
        )

    try:
        # Create and log into SAMS instance
        sams_instance = SAMSapi(sams_url="http://neurocure.charite.de/sams-cgi")
        logger.debug("Created SAMS instance for URL: %s", sams_instance.sams_url)
        sams_instance.login_with_username(sams_user, sams_password)
        phenopkg = sams_instance.get_phenopacket(patientID)
        phenopgk_filtered = filter_phenopacket_by_onset(phenopkg, "earliest")
        initial_hpo_terms = extract_HPO_terms_from_phenopacket(phenopgk_filtered)

        logger.debug("Retrieved phenopacket for patientID %s: %s", patientID, initial_hpo_terms)

        # update the row with the initial phenopacket
        genotips_api.add_data(table_id=tableID, row_id=rowID,
                    data={settings.genotips_faelle_initial_HPO_field: initial_hpo_terms}
                    )
    except Exception as e:
        logger.error("Error in sams_synchro: %s", str(e))
        if response := getattr(e, "response", None):
            logger.error(f"Error details: {response.text}")
        genotips_api.add_data(table_id=tableID, row_id=rowID,
                    data={settings.genotips_faelle_initial_HPO_field: f"Error: {str(e)}"})
        # Return HTML response with error message and guide user to write email to support (oliver.kuechler@charite.de)
        return create_HTML_response(
            header="Error in SAMS Synchronization",
            paragraphs=[f"An error occurred while synchronizing with SAMS: {str(e)}",
                        "Please make sure that the patientID is correct and that the SAMS instance is accessible."],
            status_code=500,
            failure=True
        )

    return create_HTML_response(
        header="SAMS Synchronization Successful",
        paragraphs=[f"The initial phenopacket has been successfully synchronized for patientID: {patientID}.",
                    "The initial phenopacket has been added to the corresponding Baserow entry."],
        additional_html=f"<p>Imported Initial Phenopacket:</p><pre>{initial_hpo_terms}</pre>"
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5123)


# # Check if hardcoded api request works
# logger.info(f"Requesting data from Baserow: {settings.baserow_url}/api/database/rows/table/{settings.baserow_TableTarget_table_id}/?user_field_names=true")
# response = requests.get(f"{settings.baserow_url}/api/database/rows/table/{settings.baserow_TableTarget_table_id}/?user_field_names=true", 
#     headers={"Authorization": f"Token {settings.baserow_token}"})
# logger.info(f"Response: {response.json()}")


# logger.info(f"1. Adding data to Baserow as test")
# # multiple entries can be updated at the same time
# entries = [
#     {
#         "id": None,  # this will create a new row
#         "Name": "AAA-TEst",
#     },
#     {
#         "id": 2,  # this will update row with baserow id 2
#         "Name": "BBB-TEst",
#     },
# ]
# baserow_api.add_data_batch(settings.baserow_TableTarget_table_id, entries)
