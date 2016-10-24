module.exports = {
    isArray: function(object) {
        return Object.prototype.toString.call(object) == '[object Array]';
    },
    flatten: function(array, level) {
        if(typeof level == 'undefined') { level = 0; }

        var returnArray = [];

        var levelCheck = (array, level) => {
            if(level > 0 && this.isArray(array)) {
                return levelCheck(array[0], --level);
            } else {
                return this.isArray(array);
            }
        }

        array.forEach((item) => {
            if(this.isArray(item)) {
                if(!levelCheck(item, level)) {
                    returnArray.push(item);
                } else {
                    returnArray = returnArray.concat(this.flatten(item, level));
                }
            }
            else if(this.isArray(item)) {
            } else {
                returnArray.push(item);
            }
        });

        return returnArray;
    }
}