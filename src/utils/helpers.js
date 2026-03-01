export const normalizeEmail = (email) =>
    email ? email.trim().toLowerCase() : null;
  
  export const removeNulls = (arr) =>
    [...new Set(arr.filter(Boolean))];