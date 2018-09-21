#include "gdsqlite.hpp"
#include <ProjectSettings.hpp>
#include <File.hpp>
#include <cstdlib>

using namespace godot;

enum BindType {DOUBLE, INT, TEXT};

SQLite::SQLite() {
	db = nullptr;
	memory_read = false;
}

bool SQLite::open(String path) {
	// Empty path
	if (!path.strip_edges().length())
		return false;
	
	// Convert to global path
	String real_path = ProjectSettings::get_singleton()->globalize_path(path.strip_edges());

	// Open the database
	int result = sqlite3_open(real_path.utf8().get_data(), &db);

	if (result != SQLITE_OK) {
		Godot::print("Cannot open database!");
		return false;
	}

	return true;
}

bool SQLite::open_buffered(String name, PoolByteArray buffers, int64_t size) {
	if (!name.strip_edges().length()) {
		return false;
	}

	// Get file buffer
	/*
	Ref<File> file;
	file.instance();

	if (file->open(name, file->READ) != Error::OK) {
		return false;
	}

	int64_t size = file->get_len();
	PoolByteArray buffers = file->get_buffer(size);
	*/

	if (!buffers.size() || !size) {
		return false;
	}

	// Initialize memory buffer
	spmembuffer_t *p_mem = (spmembuffer_t *)calloc(1, sizeof(spmembuffer_t));
	p_mem->total = p_mem->used = size;
	p_mem->data = (char*)malloc(size + 1);
	memcpy(p_mem->data, buffers.read().ptr(), size);
	p_mem->data[size] = '\0';

	// Open database
	spmemvfs_env_init();
	int err = spmemvfs_open_db(&p_db, name.utf8().get_data(), p_mem);

	if (err != SQLITE_OK || p_db.mem != p_mem) {
		Godot::print("Cannot open buffered database!");
		return false;
	}

	memory_read = true;
	return true;
}

void SQLite::close() {
	if (db) {
		// Cannot close database!
		if (sqlite3_close_v2(db) != SQLITE_OK) {
			Godot::print("Cannot close database!");
		} else {
			db = nullptr;
		}
	}

	if (memory_read) {
		// Close virtual filesystem database
		spmemvfs_close_db(&p_db);
		spmemvfs_env_fini();
		memory_read = false;
	}
}

sqlite3_stmt* SQLite::prepare(const char* query) {
	// Get database pointer
	sqlite3 *dbs = get_handler();

	if (!dbs) {
		Godot::print("Cannot prepare query! Database is not opened.");
		return nullptr;
	}

	// Prepare the statement
	sqlite3_stmt *stmt;
	int result = sqlite3_prepare_v2(dbs, query, -1, &stmt, nullptr);

	// Cannot prepare query!
	if (result != SQLITE_OK) {
		Godot::print("SQL Error: " + String(sqlite3_errmsg(dbs)));
		return nullptr;
	}

	return stmt;
}

bool SQLite::bind_int(sqlite3_stmt* stmt, int param_index, int value) {
	if (sqlite3_bind_int(stmt, param_index, value)) {
		return true;
	}

	return false;
}

bool SQLite::query(String query) {
	sqlite3_stmt *stmt = prepare(query.utf8().get_data());

	// Failed to prepare the query
	if (!stmt) {
		return false;
	}

	// Evaluate the sql query
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	return true;
}

Array SQLite::fetch_rows(String statement, int result_type) {
	Array result;

	// Empty statement
	if (!statement.strip_edges().length()) {
		return result;
	}

	// Cannot prepare query
	sqlite3_stmt *stmt = prepare(statement.strip_edges().utf8().get_data());
	if (!stmt) {
		return result;
	}

	// Fetch rows
	while (sqlite3_step(stmt) == SQLITE_ROW) {
		// Do a step
		result.append(parse_row(stmt, result_type));
	}

	// Delete prepared statement
	sqlite3_finalize(stmt);

	// Return the result
	return result;
}

Dictionary SQLite::parse_row(sqlite3_stmt *stmt, int result_type) {
	Dictionary result;

	// Get column count
	int col_count = sqlite3_column_count(stmt);

	// Fetch all column
	for (int i = 0; i < col_count; i++) {
		// Key name
		const char *col_name = sqlite3_column_name(stmt, i);
		String key = String(col_name);

		// Value
		int col_type = sqlite3_column_type(stmt, i);
		Variant value;

		// Get column value
		switch (col_type) {
			case SQLITE_INTEGER:
				value = Variant(sqlite3_column_int(stmt, i));
				break;

			case SQLITE_FLOAT:
				value = Variant(sqlite3_column_double(stmt, i));
				break;

			case SQLITE_TEXT:
				value = Variant((char *) sqlite3_column_text(stmt, i));
				break;

			default:
				break;
		}

		// Set dictionary value
		if (result_type == RESULT_NUM)
			result[i] = value;
		else if (result_type == RESULT_ASSOC)
			result[key] = value;
		else {
			result[i] = value;
			result[key] = value;
		}
	}

	return result;
}

Array SQLite::fetch_array(String query) {
	return fetch_rows(query, RESULT_BOTH);
}

Array SQLite::fetch_assoc(String query) {
	return fetch_rows(query, RESULT_ASSOC);
}

SQLite::~SQLite() {
	// Close database
	close();
}

void SQLite::_register_methods() {
	// Method list
	register_method("open", &SQLite::open);
	register_method("open_buffered", &SQLite::open_buffered);
	register_method("query", &SQLite::query);
	register_method("close", &SQLite::close);
	register_method("fetch_array", &SQLite::fetch_array);
	register_method("fetch_assoc", &SQLite::fetch_assoc);
}
