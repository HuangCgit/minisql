#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf + offset, static_cast<uint32_t>(fields_.size()));
  offset += sizeof(uint32_t);
  uint32_t bool_map = 0;
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      bool_map |= (1 << (fields_.size() - i - 1));
    }
  }
  MACH_WRITE_UINT32(buf + offset, bool_map);
  offset += sizeof(uint32_t);
  for (size_t i = 0; i < fields_.size(); i++) {
    offset += fields_[i]->SerializeTo(buf + offset);
  }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t field_count = MACH_READ_UINT32(buf + offset);
  fields_.resize(static_cast<size_t>(field_count));
  offset += sizeof(uint32_t);
  uint32_t bool_map = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  for (size_t i = 0; i < field_count; i++) {
    bool is_null = static_cast<bool>(bool_map & (1 << (field_count - i - 1)));
    offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], is_null);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset = sizeof(uint32_t) * 2;
  for (size_t i = 0; i < fields_.size(); i++) {
    offset += fields_[i]->GetSerializedSize();
  }
  return offset;
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
