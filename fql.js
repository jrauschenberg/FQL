// Place your code here:

// Adds properties of obj2 into obj1
function merge (obj1, obj2) {
	var result = {},
		keys = Object.keys(obj1).concat(Object.keys(obj2));
	keys.forEach(function (key) {
		result[key] = obj2.hasOwnProperty(key) ? obj2[key] : obj1[key];
	});
	return result;
}


function FQL (data) {
	this.data = data;
	this.indices = {};
}

FQL.prototype.exec = function () {
	return this.data;
}

FQL.prototype.count = function () {
	return this.exec().length;
}

FQL.prototype.limit = function (n) {
	return new FQL(this.data.slice(0,n));
}

function isPartialMatch (obj, partial) {
	for (var k in partial) {
		if (partial.hasOwnProperty(k) && partial[k] != obj[k]) {
			if (typeof partial[k] === 'function' && partial[k](obj[k])) {
				continue;
			}
			return false;
		}
	}
	return true;
}

FQL.prototype.where = function (spec) {
	for (var k in spec) {
		if (spec.hasOwnProperty(k) && this.indices[k]) {
			var self = this;
			var _new = new FQL(this.getIndicesOf(k, spec[k]).map(function (index) {
				return self.data[index];
			}));
			delete spec[k];
			return _new.where(spec);
		}
	}
	if(Object.keys(spec).length > 0) {
		var filtered = this.data.filter(function (datum) {
			return isPartialMatch(datum, spec);
		});
		return new FQL(filtered);
	} else {
		return this;
	}
}

FQL.prototype.select = function (arr) {
	return new FQL(this.data.map(function (datum) {
		var result = {};
		arr.forEach(function (field) {
			result[field] = datum[field];
		});
		return result;
	}));
}

FQL.prototype.order = function (field) {
	return new FQL(this.data.sort(function (a, b) {
		return a[field] - b[field];
	}));
}

FQL.prototype.left_join = function (fql, pred) {
	var newData = [];
	this.data.forEach(function (leftDatum) {
		fql.data.forEach(function (rightDatum) {
			if (pred(leftDatum, rightDatum)) {
				newData.push(merge(leftDatum, rightDatum));
			}
		})
	})
	return new FQL(newData);
}

FQL.prototype.addIndex = function (field) {
	var subIndices = this.indices[field] = {};
	this.data.forEach(function (datum, index) {
		var val = datum[field];
		subIndices[val] = subIndices[val] || [];
		subIndices[val].push(index);
	});
}

FQL.prototype.getIndicesOf = function (field, val) {
	if (this.indices[field]) {
		return this.indices[field][val];
	}
}