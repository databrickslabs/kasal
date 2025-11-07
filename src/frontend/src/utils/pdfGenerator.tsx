import React from 'react';
import { Document, Page, Text, View, StyleSheet, Font, pdf, Svg, Path } from '@react-pdf/renderer';
import { Run } from '../api/ExecutionHistoryService';

/* eslint-disable react/prop-types */

// Register professional fonts
Font.register({
  family: 'Inter',
  fonts: [
    { src: 'https://fonts.gstatic.com/s/inter/v12/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuLyfAZ9hjp-Ek-_EeA.woff' },
    { src: 'https://fonts.gstatic.com/s/inter/v12/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuGKYAZ9hjp-Ek-_EeA.woff', fontWeight: 600 },
    { src: 'https://fonts.gstatic.com/s/inter/v12/UcCO3FwrK3iLTeHuS_fvQtMwCp50KnMw2boKoduKmMEVuFuYAZ9hjp-Ek-_EeA.woff', fontWeight: 700 },
  ],
});

Font.register({
  family: 'JetBrainsMono',
  src: 'https://fonts.gstatic.com/s/jetbrainsmono/v13/tDbY2o-flEEny0FZhsfKu5WU4zr3E_BX0PnT8RD8yKxjPVmUsaaDhw.ttf',
});

// Create elegant styles with modern design
const styles = StyleSheet.create({
  page: {
    padding: 50,
    backgroundColor: '#ffffff',
    fontFamily: 'Inter',
  },
  // Header section with gradient-like effect
  header: {
    backgroundColor: '#f8fafb',
    borderLeft: 4,
    borderLeftColor: '#3b82f6',
    padding: 25,
    marginBottom: 30,
    borderRadius: 8,
  },
  headerTop: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 15,
  },
  logo: {
    width: 40,
    height: 40,
  },
  title: {
    fontSize: 28,
    marginBottom: 12,
    color: '#111827',
    fontFamily: 'Inter',
    fontWeight: 700,
    letterSpacing: -0.5,
  },
  subtitle: {
    fontSize: 14,
    marginBottom: 6,
    color: '#6b7280',
    fontFamily: 'Inter',
    fontWeight: 400,
  },
  statusBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: '#10b981',
    paddingHorizontal: 12,
    paddingVertical: 4,
    borderRadius: 12,
    alignSelf: 'flex-start',
  },
  statusText: {
    color: '#ffffff',
    fontSize: 11,
    fontWeight: 600,
    fontFamily: 'Inter',
  },
  // Content styles
  text: {
    fontSize: 11,
    marginBottom: 12,
    color: '#374151',
    fontFamily: 'Inter',
    lineHeight: 1.7,
  },
  section: {
    marginBottom: 25,
    paddingBottom: 20,
    borderBottom: 1,
    borderBottomColor: '#e5e7eb',
  },
  sectionTitle: {
    fontSize: 16,
    marginBottom: 12,
    color: '#111827',
    fontFamily: 'Inter',
    fontWeight: 600,
    borderLeft: 3,
    borderLeftColor: '#3b82f6',
    paddingLeft: 12,
  },
  // Code block with syntax highlighting effect
  codeBlock: {
    backgroundColor: '#1e293b',
    color: '#e2e8f0',
    padding: 15,
    borderRadius: 6,
    marginBottom: 12,
    fontFamily: 'JetBrainsMono',
    fontSize: 9,
    lineHeight: 1.6,
  },
  inlineCode: {
    backgroundColor: '#f3f4f6',
    color: '#dc2626',
    paddingHorizontal: 4,
    paddingVertical: 1,
    borderRadius: 3,
    fontFamily: 'JetBrainsMono',
    fontSize: 10,
  },
  // Lists
  list: {
    marginLeft: 24,
    marginBottom: 12,
  },
  listItem: {
    fontSize: 11,
    marginBottom: 6,
    color: '#374151',
    fontFamily: 'Inter',
    flexDirection: 'row',
  },
  listBullet: {
    width: 20,
    fontSize: 11,
    color: '#3b82f6',
  },
  // Links and emphasis
  link: {
    color: '#2563eb',
    textDecoration: 'underline',
  },
  strong: {
    fontWeight: 600,
    color: '#111827',
  },
  emphasis: {
    fontStyle: 'italic',
    color: '#4b5563',
  },
  // Blockquote
  blockquote: {
    borderLeft: 3,
    borderLeftColor: '#d1d5db',
    paddingLeft: 15,
    marginLeft: 0,
    marginBottom: 12,
    fontStyle: 'italic',
    color: '#6b7280',
  },
  // Table styles
  table: {
    marginBottom: 15,
  },
  tableRow: {
    flexDirection: 'row',
    borderBottom: 1,
    borderBottomColor: '#e5e7eb',
  },
  tableHeader: {
    backgroundColor: '#f9fafb',
    paddingVertical: 8,
    paddingHorizontal: 10,
    fontWeight: 600,
    fontSize: 10,
    color: '#111827',
  },
  tableCell: {
    paddingVertical: 8,
    paddingHorizontal: 10,
    fontSize: 10,
    color: '#374151',
  },
  // Footer
  footer: {
    position: 'absolute',
    bottom: 30,
    left: 50,
    right: 50,
    paddingTop: 15,
    borderTop: 1,
    borderTopColor: '#e5e7eb',
    flexDirection: 'row',
    justifyContent: 'space-between',
  },
  footerText: {
    fontSize: 9,
    color: '#9ca3af',
    fontFamily: 'Inter',
  },
  pageNumber: {
    fontSize: 9,
    color: '#9ca3af',
    fontFamily: 'Inter',
  },
});

interface PDFDocumentProps {
  run: Run;
}

// Helper function to get status color
const getStatusColor = (status: string): string => {
  switch (status?.toLowerCase()) {
    case 'completed':
    case 'success':
      return '#10b981';
    case 'failed':
    case 'error':
      return '#ef4444';
    case 'running':
    case 'in_progress':
      return '#3b82f6';
    case 'pending':
    case 'queued':
      return '#f59e0b';
    default:
      return '#6b7280';
  }
};

// Parse and render markdown content for PDF
const renderMarkdownContent = (text: string): React.ReactElement => {
  // Split content into lines for processing
  const lines = text.split('\n');
  const elements: React.ReactElement[] = [];
  let currentParagraph: string[] = [];
  
  const flushParagraph = () => {
    if (currentParagraph.length > 0) {
      const content = currentParagraph.join(' ').trim();
      if (content) {
        elements.push(
          <Text key={elements.length} style={styles.text}>
            {processInlineFormatting(content)}
          </Text>
        );
      }
      currentParagraph = [];
    }
  };

  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    const trimmedLine = line.trim();
    
    // Skip empty lines
    if (!trimmedLine) {
      flushParagraph();
      i++;
      continue;
    }
    
    // Handle lines starting with # as section headers in the content
    if (trimmedLine.startsWith('#')) {
      flushParagraph();
      // Remove the # symbols and any following space
      const content = trimmedLine.replace(/^#+\s*/, '').trim();
      
      // Determine the style based on number of # symbols
      const hashCount = (trimmedLine.match(/^#+/) || [''])[0].length;
      let textStyle;
      
      if (hashCount === 1) {
        // Single # - main header
        textStyle = { ...styles.sectionTitle, fontSize: 16, marginTop: 20, marginBottom: 10 };
      } else if (hashCount === 2) {
        // Double ## - subheader
        textStyle = { ...styles.sectionTitle, fontSize: 14, marginTop: 15, marginBottom: 8 };
      } else {
        // Triple or more ### - smaller subheader
        textStyle = { ...styles.text, fontWeight: 600, marginTop: 10, marginBottom: 5 };
      }
      
      elements.push(
        <Text key={elements.length} style={textStyle}>
          {processInlineFormatting(content)}
        </Text>
      );
      i++;
      continue;
    }
    
    // Handle numbered lists (1. 2. 3. etc)
    if (trimmedLine.match(/^\d+\.\s+/)) {
      flushParagraph();
      const listStartIndex = i;
      const numberedItems: Array<{number: string, content: string}> = [];
      
      while (i < lines.length) {
        const currentLine = lines[i].trim();
        const numberMatch = currentLine.match(/^(\d+)\.\s+(.+)/);
        
        if (numberMatch) {
          // Start of a numbered item
          numberedItems.push({
            number: numberMatch[1],
            content: processInlineFormatting(numberMatch[2])
          });
          i++;
        } else if (currentLine && i > listStartIndex && lines[i].startsWith('  ')) {
          // Continuation of previous item (indented)
          if (numberedItems.length > 0) {
            numberedItems[numberedItems.length - 1].content += ' ' + processInlineFormatting(currentLine);
          }
          i++;
        } else {
          // End of list
          break;
        }
      }
      
      if (numberedItems.length > 0) {
        elements.push(
          <View key={elements.length} style={styles.list}>
            {numberedItems.map((item, idx) => (
              <View key={idx} style={styles.listItem}>
                <Text style={{ ...styles.listBullet, minWidth: 25 }}>{item.number}.</Text>
                <Text style={{ ...styles.text, flex: 1 }}>{item.content}</Text>
              </View>
            ))}
          </View>
        );
      }
      continue;
    }
    
    // Handle bullet lists (- * +)
    if (trimmedLine.match(/^[-*+]\s+/)) {
      flushParagraph();
      const bulletItems: string[] = [];
      
      while (i < lines.length) {
        const currentLine = lines[i].trim();
        const bulletMatch = currentLine.match(/^[-*+]\s+(.+)/);
        
        if (bulletMatch) {
          bulletItems.push(processInlineFormatting(bulletMatch[1]));
          i++;
        } else if (currentLine && lines[i].startsWith('  ')) {
          // Continuation of previous item
          if (bulletItems.length > 0) {
            bulletItems[bulletItems.length - 1] += ' ' + processInlineFormatting(currentLine);
          }
          i++;
        } else {
          break;
        }
      }
      
      if (bulletItems.length > 0) {
        elements.push(
          <View key={elements.length} style={styles.list}>
            {bulletItems.map((item, idx) => (
              <View key={idx} style={styles.listItem}>
                <Text style={styles.listBullet}>•</Text>
                <Text style={{ ...styles.text, flex: 1 }}>{item}</Text>
              </View>
            ))}
          </View>
        );
      }
      continue;
    }
    
    // Handle code blocks (indented with 2+ spaces or tabs)
    if (line.startsWith('  ') || line.startsWith('\t')) {
      flushParagraph();
      const codeLines: string[] = [];
      
      while (i < lines.length && (lines[i].startsWith('  ') || lines[i].startsWith('\t'))) {
        codeLines.push(lines[i].replace(/^( {2}|\t)/, ''));
        i++;
      }
      
      if (codeLines.length > 0) {
        elements.push(
          <View key={elements.length} style={styles.codeBlock}>
            <Text style={{ color: '#e2e8f0', fontFamily: 'JetBrainsMono', fontSize: 9 }}>
              {codeLines.join('\n')}
            </Text>
          </View>
        );
      }
      continue;
    }
    
    // Handle blockquotes
    if (trimmedLine.startsWith('>')) {
      flushParagraph();
      const quoteLines: string[] = [];
      
      while (i < lines.length && lines[i].trim().startsWith('>')) {
        quoteLines.push(lines[i].trim().substring(1).trim());
        i++;
      }
      
      elements.push(
        <View key={elements.length} style={styles.blockquote}>
          <Text style={{ ...styles.text, color: '#6b7280', fontStyle: 'italic' }}>
            {quoteLines.join(' ')}
          </Text>
        </View>
      );
      continue;
    }
    
    // Handle horizontal rules
    if (trimmedLine.match(/^(-{3,}|\*{3,}|_{3,})$/)) {
      flushParagraph();
      elements.push(
        <View key={elements.length} style={{ borderBottom: 1, borderBottomColor: '#e5e7eb', marginVertical: 15 }} />
      );
      i++;
      continue;
    }
    
    // Regular paragraph text
    currentParagraph.push(trimmedLine);
    i++;
  }
  
  // Flush any remaining paragraph
  flushParagraph();
  
  return <View>{elements}</View>;
};

// Process inline formatting (bold, italic, code)
const processInlineFormatting = (text: string): string => {
  // First handle bold markers (** or __)
  text = text.replace(/\*\*([^*]+)\*\*/g, '$1');
  text = text.replace(/__([^_]+)__/g, '$1');
  
  // Then handle italic markers (* or _) - but not if they're part of bold
  text = text.replace(/(?<!\*)\*(?!\*)([^*]+)(?<!\*)\*(?!\*)/g, '$1');
  text = text.replace(/(?<!_)_(?!_)([^_]+)(?<!_)_(?!_)/g, '$1');
  
  // Handle inline code
  text = text.replace(/`([^`]+)`/g, '$1');
  
  // Handle links - keep text, remove URL
  text = text.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1');
  
  // Clean up any escaped characters
  text = text.replace(/\\([*_`[\]()])/g, '$1');
  
  return text;
};

// Format key names to be more readable
const formatKeyName = (key: string): string => {
  return key
    .replace(/_/g, ' ')
    .replace(/([A-Z])/g, ' $1')
    .trim()
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
};

const renderContent = (result: unknown, depth = 0): React.ReactElement => {
  if (!result) {
    return (
      <View style={{ padding: 20, backgroundColor: '#f9fafb', borderRadius: 6 }}>
        <Text style={{ ...styles.text, color: '#9ca3af', textAlign: 'center' }}>
          No results available
        </Text>
      </View>
    );
  }

  if (typeof result === 'string') {
    // Check if it's JSON string
    try {
      const parsed = JSON.parse(result);
      return renderContent(parsed, depth);
    } catch {
      // It's a regular string, possibly markdown
      return renderMarkdownContent(result);
    }
  }

  if (Array.isArray(result)) {
    return (
      <View>
        {result.map((item, index) => (
          <View key={index} style={{ marginBottom: 10 }}>
            <View style={{ flexDirection: 'row', alignItems: 'center', marginBottom: 5 }}>
              <View style={{ 
                width: 24, 
                height: 24, 
                backgroundColor: '#3b82f6', 
                borderRadius: 12,
                alignItems: 'center',
                justifyContent: 'center',
                marginRight: 8
              }}>
                <Text style={{ color: '#ffffff', fontSize: 10, fontWeight: 600 }}>
                  {index + 1}
                </Text>
              </View>
            </View>
            {renderContent(item, depth + 1)}
          </View>
        ))}
      </View>
    );
  }

  if (typeof result === 'object' && result !== null) {
    // Handle nested data structures
    if ('data' in result && result.data) {
      return renderContent(result.data, depth);
    }

    const entries = Object.entries(result).filter(([key]) => 
      !['_id', '__v', 'createdAt', 'updatedAt'].includes(key)
    );

    if (entries.length === 0) {
      return <Text style={styles.text}>Empty object</Text>;
    }

    return (
      <View>
        {entries.map(([key, value]) => {
          const isComplexValue = typeof value === 'object' && value !== null;
          
          return (
            <View key={key} style={{ marginBottom: 15 }}>
              {depth === 0 ? (
                <Text style={styles.sectionTitle}>{formatKeyName(key)}</Text>
              ) : (
                <Text style={{ ...styles.text, fontWeight: 600, marginBottom: 5 }}>
                  {formatKeyName(key)}:
                </Text>
              )}
              
              {typeof value === 'string' ? (
                // Always render strings as markdown content, not as code blocks
                renderMarkdownContent(value)
              ) : isComplexValue ? (
                <View style={{ paddingLeft: depth > 0 ? 15 : 0 }}>
                  {renderContent(value, depth + 1)}
                </View>
              ) : (
                <Text style={styles.text}>{String(value)}</Text>
              )}
            </View>
          );
        })}
      </View>
    );
  }

  // Fallback for primitive types
  return <Text style={styles.text}>{String(result)}</Text>;
};

const PDFDocument: React.FC<PDFDocumentProps> = ({ run }) => {
  const statusColor = getStatusColor(run.status);
  const currentDate = new Date().toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });

  return (
    <Document>
      <Page size="A4" style={styles.page}>
        {/* Elegant Header */}
        <View style={styles.header}>
          <View style={styles.headerTop}>
            <View style={{ flex: 1 }}>
              <Text style={styles.title}>{run.run_name}</Text>
            </View>
            {/* Logo placeholder - you can add an actual logo here */}
            <Svg width="40" height="40" viewBox="0 0 40 40">
              <Path
                d="M20 5 L35 15 L35 25 L20 35 L5 25 L5 15 Z"
                fill="#3b82f6"
                opacity="0.1"
              />
              <Path
                d="M20 10 L30 17 L30 23 L20 30 L10 23 L10 17 Z"
                fill="#3b82f6"
              />
            </Svg>
          </View>
          <View style={{ marginTop: 12 }}>
            <Text style={styles.subtitle}>
              {new Date(run.created_at).toLocaleDateString('en-US', {
                weekday: 'long',
                year: 'numeric',
                month: 'long',
                day: 'numeric',
              })}
            </Text>
            {run.completed_at && run.status?.toLowerCase() !== 'completed' && (
              <View style={{ flexDirection: 'row', alignItems: 'center', marginTop: 8 }}>
                <View style={{ ...styles.statusBadge, backgroundColor: statusColor }}>
                  <Text style={styles.statusText}>
                    {run.status?.toUpperCase() || 'UNKNOWN'}
                  </Text>
                </View>
              </View>
            )}
          </View>
        </View>

        {/* Main Content */}
        <View style={{ marginTop: 20, flex: 1 }}>
          {renderContent(run.result)}
        </View>

        {/* Footer */}
        <View style={styles.footer}>
          <Text style={styles.footerText}>
            Generated on {currentDate}
          </Text>
          <Text style={styles.pageNumber}>
            Page 1
          </Text>
        </View>
      </Page>
    </Document>
  );
};

export const generateRunPDF = async (run: Run): Promise<void> => {
  try {
    const blob = await pdf(<PDFDocument run={run} />).toBlob();
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    const sanitizedName = run.run_name.replace(/[^a-z0-9]/gi, '_').toLowerCase();
    const timestamp = new Date().toISOString().split('T')[0];
    link.download = `${sanitizedName}_${timestamp}_${run.job_id}.pdf`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  } catch (error) {
    console.error('Error generating PDF:', error);
    throw error;
  }
}; 