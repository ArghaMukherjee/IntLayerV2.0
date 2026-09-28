-- =============================================================================
-- Example workflows. Add or update more through PUT /v1/admin/workflows/{name}.
--   order_validation  WWMG_Source orders            -> stored in PostgreSQL
--   care_request      MyCareHub member requests     -> stored in BigQuery
-- =============================================================================

INSERT INTO integration.workflow_registry
    (workflow_name, target_app, description, max_retries, storage_backend, input_schema, output_schema)
VALUES (
    'order_validation',
    'ConsumerApplication',
    'WWMG_Source submits an order; ConsumerApplication validates it and returns an approval decision.',
    3,
    'postgres',
    $json${
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": ["order_id", "customer_id", "currency", "items"],
      "properties": {
        "order_id":    {"type": "string", "minLength": 1, "maxLength": 64},
        "customer_id": {"type": "string", "minLength": 1, "maxLength": 64},
        "currency":    {"type": "string", "pattern": "^[A-Z]{3}$"},
        "order_date":  {"type": "string", "format": "date"},
        "items": {
          "type": "array", "minItems": 1, "maxItems": 500,
          "items": {
            "type": "object",
            "additionalProperties": false,
            "required": ["sku", "quantity", "unit_price"],
            "properties": {
              "sku":        {"type": "string", "minLength": 1},
              "quantity":   {"type": "integer", "minimum": 1},
              "unit_price": {"type": "number", "minimum": 0}
            }
          }
        }
      }
    }$json$::jsonb,
    $json${
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "required": ["order_id", "decision", "total_amount", "currency"],
      "properties": {
        "order_id":     {"type": "string"},
        "decision":     {"enum": ["APPROVED", "REJECTED"]},
        "total_amount": {"type": "number", "minimum": 0},
        "currency":     {"type": "string", "pattern": "^[A-Z]{3}$"},
        "reasons":      {"type": "array", "items": {"type": "string"}}
      }
    }$json$::jsonb
)
ON CONFLICT (workflow_name) DO NOTHING;

INSERT INTO integration.workflow_registry
    (workflow_name, target_app, description, max_retries, storage_backend, input_schema, output_schema)
VALUES (
    'care_request',
    'ConsumerApplication',
    'A MyCareHub member asks for care (appointment, refill, referral, lab results); ConsumerApplication triages it.',
    3,
    'bigquery',
    $json${
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "additionalProperties": false,
      "required": ["member_id", "request_type", "urgency"],
      "properties": {
        "member_id":      {"type": "string", "pattern": "^M-[0-9]{4}$"},
        "request_type":   {"enum": ["appointment", "prescription_refill", "referral", "lab_results"]},
        "urgency":        {"enum": ["routine", "soon", "urgent"]},
        "preferred_date": {"type": "string", "format": "date"},
        "notes":          {"type": "string", "maxLength": 500}
      }
    }$json$::jsonb,
    $json${
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "required": ["member_id", "request_type", "decision", "message", "reference"],
      "properties": {
        "member_id":     {"type": "string"},
        "request_type":  {"type": "string"},
        "decision":      {"enum": ["SCHEDULED", "APPROVED", "SUBMITTED", "RELEASED", "ESCALATED"]},
        "message":       {"type": "string"},
        "reference":     {"type": "string"},
        "scheduled_for": {"type": "string"},
        "ready_by":      {"type": "string"}
      }
    }$json$::jsonb
)
ON CONFLICT (workflow_name) DO NOTHING;
