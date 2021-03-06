#include "gdsqlite.hpp"
#include <ProjectSettings.hpp>
#include <File.hpp>

using namespace godot;

enum BindType { DOUBLE, INT, TEXT };

SQLite::SQLite() {
	db = nullptr;
	memory_read = false;
}

bool SQLite::open(String path) {
	// Empty path
	if (!path.strip_edges().length())
		return false;
	
	// Convert to global path
	String real_path = ProjectSettings::globalize_path(path.strip_edges());

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

	Godot::print(query);

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

bool SQLite::bind_params(sqlite3_stmt *stmt, Array params, Array types) {
	if (params.size() != types.size()) {
		Godot::print("Binding failed: param and type arrays size mismatch.");
		return false;
	}

	int param_index = 1;

	while (!types.empty()) {
		if (types.front().get_type() != GODOT_VARIANT_TYPE_INT) {
			Godot::print("Binding failed: bad type.");
			return false;
		}

		int type = types.pop_front();

		if (type == DOUBLE) {
			if (params.front().get_type() != GODOT_VARIANT_TYPE_REAL) {
				Godot::print("Binding failed: type mismatch.");
				return false;
			}

			sqlite3_bind_double(stmt, param_index, params.pop_front());
		}
		else if (type == INT) {
			if (params.front().get_type() != GODOT_VARIANT_TYPE_INT) {
				Godot::print("Binding failed: type mismatch.");
				return false;
			}

			sqlite3_bind_int(stmt, param_index, params.pop_front());
		}
		else if (type == TEXT) {
			if (params.front().get_type() != GODOT_VARIANT_TYPE_STRING) {
				Godot::print("Binding failed: type mismatch.");
				return false;
			}

			String text = params.pop_front();
			sqlite3_bind_text(stmt, param_index, text.utf8().get_data(), -1, SQLITE_TRANSIENT);
		}
		else {
			Godot::print("Binding failed: invalid type.");
			return false;
		}

		param_index++;
	}

	return true;
}

bool SQLite::simple_query(String query) {
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

bool SQLite::query(String query, Array params, Array types) {
	sqlite3_stmt *stmt = prepare(query.utf8().get_data());

	// Failed to prepare the query
	if (!stmt) {
		return false;
	}

	if(!bind_params(stmt, params, types)) {
		return false;
	}

	// Evaluate the sql query
	sqlite3_step(stmt);
	sqlite3_finalize(stmt);

	return true;
}

sqlite3_stmt* SQLite::fetch_prepare(String statement) {
	// Empty statement
	if (!statement.strip_edges().length()) {
		return 0;
	}

	// Cannot prepare query
	sqlite3_stmt *stmt = prepare(statement.strip_edges().utf8().get_data());
	if (!stmt) {
		return 0;
	}

	return stmt;
}

Array SQLite::fetch_rows(sqlite3_stmt* stmt, int result_type) {
	Array result;

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

Array SQLite::simple_fetch_array(String query) {
	sqlite3_stmt* stmt = fetch_prepare(query);

	if (!stmt) { 
		return Array(); 
	}

	return fetch_rows(stmt, RESULT_BOTH);
}

Array SQLite::simple_fetch_assoc(String query) {
	sqlite3_stmt* stmt = fetch_prepare(query);

	if (!stmt) {
		return Array();
	}

	return fetch_rows(stmt, RESULT_ASSOC);
}

Array SQLite::fetch_array(String query, Array params, Array types) {
	sqlite3_stmt* stmt = fetch_prepare(query);

	if (!stmt) {
		Godot::print("Fetch prepare failed.");
		return Array();
	}

	if (!bind_params(stmt, params, types)) {
		Godot::print("Fetch bind failed.");
		return Array();
	}

	return fetch_rows(stmt, RESULT_BOTH);
}

Array SQLite::fetch_assoc(String query, Array params, Array types) {
	sqlite3_stmt* stmt = fetch_prepare(query);

	if (!stmt) {
		Godot::print("Fetch prepare failed.");
		return Array();
	}

	if (!bind_params(stmt, params, types)) {
		Godot::print("Fetch bind failed.");
		return Array();
	}

	return fetch_rows(stmt, RESULT_ASSOC);
}

SQLite::~SQLite() {
	// Close database
	close();
}

void SQLite::_register_methods() {
	// Method list
	register_method("open", &SQLite::open);
	register_method("open_buffered", &SQLite::open_buffered);
	register_method("simple_query", &SQLite::simple_query);
	register_method("query", &SQLite::query);
	register_method("close", &SQLite::close);
	register_method("simple_fetch_array", &SQLite::simple_fetch_array);
	register_method("simple_fetch_assoc", &SQLite::simple_fetch_assoc);
	register_method("fetch_array", &SQLite::fetch_array);
	register_method("fetch_assoc", &SQLite::fetch_assoc);
}
