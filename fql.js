var fs = require('fs');

// _readTable takes a string representing a table name
// and returns an array of objects, namely the rows.
// It does so by looking up actual files, reading them,
// and parsing them from JSON strings into JS objects.
function _readTable (tableName) {
	var folderName = __dirname + '/film-database/' + tableName;
	var fileNames = fs.readdirSync(folderName);
	var fileStrings = fileNames.map(function (fileName) {
		var filePath = folderName + '/' + fileName;
		return fs.readFileSync(filePath).toString();
	});
	var table = fileStrings.map(function (fileStr) {
		return JSON.parse(fileStr);
	});
	return table;
}

function merge (obj1, obj2) {
	newObj = obj1;
	for(var key in obj2) {
		if (!newObj[key]) {
			newObj[key] = obj2[key];
		} else {
			//assuming no overlap, so not doing anything with it...
		}
	}
	return newObj;
}

function FQL (table) {
	this.table = table;
}


FQL.prototype.exec = function () {
	return this.table;
}

FQL.prototype.count = function () {
	return this.table.length;
}

FQL.prototype.limit = function (n) {
	return new FQL(this.table.slice(0,n));
}

FQL.prototype.where = function (truthObj) {
	var keys = Object.keys(truthObj);
	var passingArray = this.table.filter(function (item) {
		var counter = keys.length;
		// var truthCounter = 0;
		for (var i = 0; i < counter; i++) {
			if (typeof truthObj[keys[i]] === 'function') {
				if(!truthObj[keys[i]](item[keys[i]])) {
					return false;
				}
			} else {
				if (item[keys[i]] !== truthObj[keys[i]]) {
					return false;
				}
			}
		}
		return true;
	});
	return new FQL(passingArray);
}



module.exports = {
	FQL: FQL,
	merge: merge,
	_readTable: _readTable
};