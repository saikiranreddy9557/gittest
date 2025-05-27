import psycopg2
from psycopg2 import OperationalError

def get_db_connection(
    dbname="testdb",
    user="user",
    password="password",
    host="localhost",
    port="5433"
):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("✅ Connection to database established.")
        return conn
    except OperationalError as e:
        print(f"Failed to connect to database: {e}")
        return None




import psycopg2
from psycopg2 import OperationalError

def create_alerts_table_with_indexes():
    try:
       
        conn = get_db_connection()
        cur = conn.cursor()
        cur = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS alerts (
            id SERIAL PRIMARY KEY,
            account_id INTEGER NULL,
            account_name TEXT NULL,
            closed_violations_count_critical INTEGER NULL,
            closed_violations_count_warning INTEGER NULL,
            condition_description TEXT NULL,
            condition_family_id INTEGER NULL,
            condition_name TEXT NULL,
            current_state TEXT NULL,
            details TEXT NULL,
            duration BIGINT NULL,
            event_type TEXT NULL,
            incident_acknowledge_url TEXT NULL,
            incident_id INTEGER NULL,
            incident_url TEXT NULL,
            metadata JSONB NULL,
            open_violations_count_critical INTEGER NULL,
            open_violations_count_warning INTEGER NULL,
            owner TEXT NULL,
            category TEXT NULL,
            ticket_created TIMESTAMP NULL,
            policy_name TEXT NULL,
            policy_url TEXT NULL,
            runbook_url TEXT NULL,
            severity TEXT NULL,
            targets TEXT NULL,
            timestamp TIMESTAMP NULL,
            violation_callback_url TEXT NULL,
            violation_chart_url TEXT NULL,
            ticket_created_SN BOOLEAN
        );
        """

        create_index_condition_name = """
        CREATE INDEX IF NOT EXISTS idx_condition_name ON alerts(condition_name);
        """

        create_index_timestamp = """
        CREATE INDEX IF NOT EXISTS idx_timestamp ON alerts(timestamp);
        """

        cur.execute(create_table_query)
        cur.execute(create_index_condition_name)
        cur.execute(create_index_timestamp)

        conn.commit()
        print("✅ Table 'alerts' and indexes created successfully (or already exist).")

        cur.close()
        conn.close()
    except OperationalError as e:
          print(f" Unable to connect or execute query: {e}")


create_alerts_table_with_indexes()


from datetime import datetime
import psycopg2
import json

def insert_alert(data):
    try:
        conn = psycopg2.connect(
            dbname="testdb",
            user="user",
            password="password",
            host="localhost",
            port="5433"
        )
        cur = conn.cursor()

        insert_query = """
        INSERT INTO alerts (
            account_id, account_name, closed_violations_count_critical, closed_violations_count_warning,
            condition_description, condition_family_id, condition_name, current_state, details, duration,
            event_type, incident_acknowledge_url, incident_id, incident_url, metadata,
            open_violations_count_critical, open_violations_count_warning, owner,
            policy_name, policy_url, runbook_url, severity, targets, timestamp, violation_callback_url,violation_chart_url
        ) VALUES (
            %(account_id)s, %(account_name)s, %(closed_violations_count_critical)s, %(closed_violations_count_warning)s,
            %(condition_description)s, %(condition_family_id)s, %(condition_name)s, %(current_state)s, %(details)s, %(duration)s,
            %(event_type)s, %(incident_acknowledge_url)s, %(incident_id)s, %(incident_url)s, %(metadata)s,
            %(open_violations_count_critical)s, %(open_violations_count_warning)s, %(owner)s,
            %(policy_name)s, %(policy_url)s, %(runbook_url)s, %(severity)s, %(targets)s, %(timestamp)s, %(violation_callback_url)s,%(violation_chart_url)s
        );
        """

        # Fix keys for database columns
        db_data = {
            "account_id": data.get("account_id"),
            "account_name": data.get("account_name"),
            "closed_violations_count_critical": data.get("closed_violations_count_critical"),
            "closed_violations_count_warning": data.get("closed_violations_count_warning"),
            "condition_description": data.get("condition_description"),
            "condition_family_id": data.get("condition_family_id"),
            "condition_name": data.get("condition_name"),
            "current_state": data.get("current_state"),
            "details": data.get("details"),
            "duration": data.get("duration"),
            "event_type": data.get("event_type") or data.get("eventType"),
            "incident_acknowledge_url": data.get("incident acknowledge_url"),
            "incident_id": data.get("incident_id"),
            "incident_url": data.get("incident_url"),
            "metadata": json.dumps(data.get("metadata")) if data.get("metadata") else None,
            "open_violations_count_critical": data.get("open_violations_count_critical"),
            "open_violations_count_warning": data.get("open_violations_count_warning"),
            "owner": data.get("owner"),
            "policy_name": data.get("policy_name"),
            "policy_url": data.get("policy_url"),
            "runbook_url": data.get("runbook_url"),
            "severity": data.get("severity"),
            "targets": data.get("targets"),
            "timestamp": datetime.fromtimestamp(data.get("timestamp") / 1000.0) if data.get("timestamp") else None,
            "violation_callback_url": data.get("violation_callback_url"),
            "violation_chart_url": data.get("violation_chart_url"),
        }

        cur.execute(insert_query, db_data)
        conn.commit()
        print("✅ Alert inserted successfully")
        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error inserting alert: {e}")


alert_data = {
    "account_id": 3356486,
    "account_name": "CDK-Core-prod",
    "closed_violations_count_critical": 80,
    "closed_violations_count_warning": 0,
    "condition_description": "Updated the alert condition and added filter in NRQL",
    "condition_family_id": 23712752,
    "condition_name": "Container Memory Usage % is too high",
    "current_state": "closed",
    "details": "Memory Used % is more than 90 for at least 2 minutes on 'Some-Entity'",
    "duration": 240000,
    "event_type": "INCIDENT",
    "eventType": "Alerts",
    "incident_acknowledge_url": "https://radar-api.service.newrelic.com/accounts/1/issues/0ea2df1c-adab-45d2-aae0-042b609d2322?notifier=SLACK&action=ACK",
    "incident_id": -1,
    "incident_url": "https://radar-api.service.newrelic.com/accounts/1/issues/0ea2df1c-adab-45d2-aae0-042b609d2322?notifier=SLACK",
    "metadata": {"section": "metadata"},
    "open_violations_count_critical": 310,
    "open_violations_count_warning": 0,
    "owner": "John Doe",
    "policy_name": "TEST | Kubernetes",
    "policy_url": "https://radar-api.staging-service.newrelic.com/accounts/1/issues/1613db7f-6d60-42be-aaa7-536b7a85f9f9?notifier-SLACK&action=",
    "runbook_url": "https://confluence.cdk.com/display/GHS/GIS+-+Internal+CDK+Networks",
    "severity": "CRITICAL",
    "targets": "sample",
    "timestamp": 1747821637853,
    "violation_callback_url": "https://radar-api.service.newrelic.com/accounts/1/issues/0ea2df1c-adab-45d2-aae0-042b689d2322?notifier=SLACK",
    "violation_chart_url": "https://radar-api.service.newrelic.com/accounts/1/issues/0ea2df1c-adab-45d2-aae0-042b689d2322?notifier=SLACK"
}

insert_alert(alert_data)

import psycopg2

def fetch_timestamps():
    try:
        conn = psycopg2.connect(
            dbname="testdb",
            user="user",
            password="password",
            host="localhost",
            port="5433"
        )
        cur = conn.cursor()

        cur.execute("SELECT id, timestamp FROM alerts ORDER BY id DESC LIMIT 5;")
        rows = cur.fetchall()

        print("Recent raw timestamps:")
        for row in rows:
            print(f"ID: {row[0]} | Epoch (ms): {row[1]}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error fetching timestamps: {e}")

fetch_timestamps()




