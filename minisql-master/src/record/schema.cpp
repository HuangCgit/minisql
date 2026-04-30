#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset = 0;
  MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + offset, static_cast<uint32_t>(columns_.size()));
  offset += sizeof(uint32_t);
  for (const auto column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t offset = sizeof(uint32_t) * 2;
  for (const auto column : columns_) {
    offset += column->GetSerializedSize();
  }
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
    uint32_t offset = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid schema magic number.");
  uint32_t column_count = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  std::vector<Column *> columns(column_count);
  for (size_t i = 0; i < column_count; i++) {
    offset += Column::DeserializeFrom(buf + offset, columns[i]);
  }
  schema = new Schema(columns);
  return offset;
}