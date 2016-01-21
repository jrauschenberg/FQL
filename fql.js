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
	newObj = {};
	for(var key in obj1) {
		newObj[key] = obj1[key];
	}
	for(var key in obj2) {
		if (!newObj[key]) {
			newObj[key] = obj2[key];
		}
	}

	return newObj;
}

function FQL (table) {
	this.table = table;
	this.index = {};
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

	// var index = this.getIndicesOf(keys[0], truthObj[keys[0]]);
	// if (index) {
 //    return new FQL(this.table[index]);
	// }

	if (this.index[keys[0]]) {
		var indexArray = this.getIndicesOf(keys[0], truthObj[keys[0]]);
	  var results = [];
	  indexArray.forEach(function(index) {
      results.push(this.table[index]);
	  }, this);
	  return new FQL(results);
	}

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

FQL.prototype.select = function(keysArray) {
  var newTable = this.table.map(function(item) {
    var tempObject = {};
    keysArray.forEach(function(key) {
      tempObject[key] = item[key];
    });
    return tempObject;
  });
  
	return new FQL(newTable);
}

FQL.prototype.order = function(key) {
	var newTable = this.table;
	newTable.sort(function(a, b) {
    if (a[key] < b[key]) {
    	return -1;
    }
    else if (a[key] > b[key]) {
    	return 1;
    } else {
    	return 0;
    }
	});

	return new FQL(newTable);
}

FQL.prototype.left_join = function(dataSet, func) {
  //check this.table against dataSet
  var leftJoinedTable = [];
  var dataSetArray = dataSet.table;
  this.table.forEach(function(movie) {
    dataSetArray.forEach(function(role) {
    	if (func(movie, role)) {
    		leftJoinedTable.push(merge(movie, role));
    	}
    });
  });

  return new FQL(leftJoinedTable);
}

FQL.prototype.addIndex = function(name) {
	var newIndex = {};
	newIndex[name] = {};
  this.table.forEach(function(movie, index) {
  	if (!newIndex[name][movie[name]]) {
      newIndex[name][movie[name]] = [index];
  	} else {
  		newIndex[name][movie[name]].push(index);
  	}
  });
  
  this.index[name] = newIndex[name];
}

FQL.prototype.getIndicesOf = function(name, movie) {
	if (!this.index[name]) return undefined;
	return this.index[name][movie];
}

module.exports = {
	FQL: FQL,
	merge: merge,
	_readTable: _readTable
};