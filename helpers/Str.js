module.exports = {
    replace: function(search, replace, subject) {
        var map = {};
        for(var a = 0; a < search.length; a++) {
            map[search[a].toLowerCase()] = replace[a];
        }
        return subject.replace(new RegExp(search.join('|'), 'gi'), function(matched){
            return map[matched.toLowerCase()];
        });
    },
    substr: function(string, start, stop) {
        string = String(string);
        if(start < 0) {
            start = string.length + start;
        }
        if(stop < 0) {
            stop = string.length + stop - start;
        }

        return string.substr(start, stop);
    },
    ucFirst: function(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }
};