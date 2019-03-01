var find = exports.find = function find(ary, test) {
  for(var i in ary)
    if(test(ary[i], i, ary)) return ary[i]
}
