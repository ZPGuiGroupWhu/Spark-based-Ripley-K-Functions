const getURLWithParam = function (url, params) {
    let paramString = '';
    for (let k in params) {
        paramString += `&${k}=${params[k]}`;
    }
    return `${url}?${paramString.substring(1)}`;
}

export {getURLWithParam};