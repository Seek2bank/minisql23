#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t offset = 0;


  // Serialize field count
  uint32_t field_count = fields_.size();
  memcpy(buf + offset, &field_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);

  // Create null bitmap
  uint32_t bitmap_size = (schema->GetColumnCount() + 7) / 8;
  char *bitmap = buf + offset;
  memset(bitmap, 0, bitmap_size);
  offset += bitmap_size;



  // Serialize fields
  for (uint32_t i = 0; i < field_count; i++) {
    Field *field = fields_[i];
    if (field == nullptr) {
      // Set corresponding bit in the null bitmap to indicate null field
      bitmap[i / 8] |= (1 << (i % 8));
    } else {
      // Serialize non-null field
      offset += field->SerializeTo(buf + offset);
    }
  }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");

  uint32_t offset = 0;

  // Deserialize field count
  uint32_t field_count = 0;
  memcpy(&field_count, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);


  // Create null bitmap
  uint32_t bitmap_size = (schema->GetColumnCount() + 7) / 8;
  const char *bitmap = buf + offset;
  offset += bitmap_size;


  // Deserialize fields
  fields_.resize(field_count);
  for (uint32_t i = 0; i < field_count; i++) {
    Field *field = nullptr;
    if (bitmap[i / 8] & (1 << (i % 8))) {
      // Field is null, set nullptr
      offset += 0;
    } else {
      // Field is non-null, deserialize the field
      offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &field, 0);
    }
    fields_[i] = field;
  }

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  uint32_t size = 0;

  // Add field count size
  size += sizeof(uint32_t);

  // Calculate null bitmap size
  uint32_t bitmap_size = (schema->GetColumnCount() + 7) / 8;
  size += bitmap_size;


  // Add size of each field
  for (Field *field : fields_) {
    size +=  field->GetSerializedSize();
  }

  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
