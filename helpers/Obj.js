module.exports = {
    clone: function(obj) {
        if (null == obj || "object" != typeof obj) return obj;
        var copy = obj.constructor();
        for (var attr in obj) {
            if (obj.hasOwnProperty(attr)) copy[attr] = obj[attr];
        }
        return copy;
    },
    /**
     * Got from: http://stackoverflow.com/a/8052100
     *
     * @param obj
     * @param desc
     * @returns {*}
     */
    dotNotation: function(obj, desc) {
        var arr = desc.split('.');
        while(arr.length && (obj = obj[arr.shift()]));
        return obj;
    },
    getType: function(object) {
        var funcNameRegex = /function (.{1,})\(/;
        if(typeof object == 'undefined') { return undefined; }
        if(object == null) { return null };
        var results = (funcNameRegex).exec((object).constructor.toString());
        return (results && results.length > 1) ? results[1] : "";
    },
    merge: function(object1, object2) {
        var returnObject = {};
        Object.keys(object1).forEach((name) => {
            returnObject[name] = object1[name];
        });
        Object.keys(object2).forEach((name) => {
            returnObject[name] = object2[name];
        });

        return returnObject;
    },
    filter: function(object, filter) {
        var returnObject = {};
        Object.keys(object).filter((value, index, array) => filter(object[value], value, object)).forEach((key) => {
            returnObject[key] = object[key];
        });

        return returnObject;
    }
}