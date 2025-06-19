// Strip ANSI escape sequences (including the ESC character)
export const stripAnsiEscapes = (text: string): string => {
  if (!text) return '';

  // Create the escape character as a string
  const ESC = String.fromCharCode(27); // ASCII 27 (ESC)
  const BEL = String.fromCharCode(7);  // ASCII 7 (BEL)

  // Process the text to remove ANSI sequences
  return text
    // Standard ANSI colors and styles 
    .replace(new RegExp(`${ESC}\\[[0-9;]*[ABCDEFGHJKLMSTfminsulh]`, 'g'), '')
    // Control sequences with BEL terminator
    .replace(new RegExp(`${ESC}\\][^${BEL}]*${BEL}`, 'g'), '')
    // Single character controls
    .replace(new RegExp(`${ESC}[NOPGKabcdfghinqrstuABCDHIJMOPRTl]`, 'g'), '')
    // Other formats
    .replace(new RegExp(`${ESC}[\\[\\]\\?\\(\\)#;0-9]*[0-9A-Za-z]`, 'g'), '')
    // Final cleanup of any remaining ESC characters
    .replace(new RegExp(ESC, 'g'), '');
};

// URL detection regex pattern
export const urlPattern = /(https?:\/\/[^\s]+)/g;

// Check if text contains markdown patterns
export const isMarkdown = (text: string): boolean => {
  const markdownPatterns = [
    /^#+ /m,           // Headers
    /\*\*.+\*\*/,      // Bold
    /_.+_/,            // Italic
    /\[.+\]\(.+\)/,    // Links
    /^\s*[-*+]\s/m,    // Lists
    /^\s*\d+\.\s/m,    // Numbered lists
    /```[\s\S]*```/,   // Code blocks
    /^\s*>/m,          // Blockquotes
  ];
  return markdownPatterns.some(pattern => pattern.test(text));
};