import { Document, Page, Text, View, StyleSheet, Font, pdf } from '@react-pdf/renderer';
import { Run } from '../api/ExecutionHistoryService';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { Components } from 'react-markdown';

/* eslint-disable react/prop-types */

// Register fonts
Font.register({
  family: 'Helvetica',
  fonts: [
    { src: 'https://cdnjs.cloudflare.com/ajax/libs/ink/3.1.10/fonts/Helvetica/helvetica_regular.afm' },
    { src: 'https://cdnjs.cloudflare.com/ajax/libs/ink/3.1.10/fonts/Helvetica/helvetica_bold.afm', fontWeight: 'bold' },
  ],
});

// Create styles
const styles = StyleSheet.create({
  page: {
    padding: 40,
    backgroundColor: '#ffffff',
  },
  header: {
    backgroundColor: '#f5f5f5',
    padding: 20,
    marginBottom: 20,
    borderRadius: 4,
  },
  title: {
    fontSize: 24,
    marginBottom: 10,
    color: '#333333',
    fontFamily: 'Helvetica',
    fontWeight: 'bold',
  },
  subtitle: {
    fontSize: 16,
    marginBottom: 5,
    color: '#666666',
    fontFamily: 'Helvetica',
  },
  text: {
    fontSize: 12,
    marginBottom: 10,
    color: '#333333',
    fontFamily: 'Helvetica',
    lineHeight: 1.5,
  },
  section: {
    marginBottom: 20,
  },
  sectionTitle: {
    fontSize: 18,
    marginBottom: 10,
    color: '#333333',
    fontFamily: 'Helvetica',
    fontWeight: 'bold',
  },
  codeBlock: {
    backgroundColor: '#f5f5f5',
    padding: 10,
    borderRadius: 4,
    marginBottom: 10,
    fontFamily: 'Courier',
    fontSize: 10,
  },
  list: {
    marginLeft: 20,
    marginBottom: 10,
  },
  listItem: {
    fontSize: 12,
    marginBottom: 5,
    color: '#333333',
    fontFamily: 'Helvetica',
  },
  link: {
    color: '#0066cc',
    textDecoration: 'underline',
  },
});

interface PDFDocumentProps {
  run: Run;
}

const renderMarkdown = (text: string) => {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        h1: ({ children }) => <Text style={styles.title}>{children}</Text>,
        h2: ({ children }) => <Text style={styles.sectionTitle}>{children}</Text>,
        h3: ({ children }) => <Text style={styles.sectionTitle}>{children}</Text>,
        p: ({ children }) => <Text style={styles.text}>{children}</Text>,
        code: ({ children }) => (
          <Text style={styles.codeBlock}>{children}</Text>
        ),
        ul: ({ children }) => <View style={styles.list}>{children}</View>,
        ol: ({ children }) => <View style={styles.list}>{children}</View>,
        li: ({ children }) => <Text style={styles.listItem}>• {children}</Text>,
        a: ({ href, children }) => (
          <Text style={styles.link}>{children}</Text>
        ),
        br: () => <Text style={styles.text}>{'\n'}</Text>,
        pre: ({ children }) => (
          <Text style={styles.codeBlock}>{children}</Text>
        ),
        strong: ({ children }) => (
          <Text style={{ ...styles.text, fontWeight: 'bold' }}>{children}</Text>
        ),
        em: ({ children }) => (
          <Text style={{ ...styles.text, fontStyle: 'italic' }}>{children}</Text>
        ),
        blockquote: ({ children }) => (
          <Text style={{ ...styles.text, marginLeft: 20, fontStyle: 'italic' }}>{children}</Text>
        ),
        hr: () => <Text style={styles.text}>{'\n---\n'}</Text>,
        table: ({ children }) => <View style={styles.section}>{children}</View>,
        thead: ({ children }) => <View style={styles.section}>{children}</View>,
        tbody: ({ children }) => <View style={styles.section}>{children}</View>,
        tr: ({ children }) => <View style={styles.section}>{children}</View>,
        th: ({ children }) => <Text style={{ ...styles.text, fontWeight: 'bold' }}>{children}</Text>,
        td: ({ children }) => <Text style={styles.text}>{children}</Text>,
        text: ({ children }) => <Text style={styles.text}>{children}</Text>,
      } as Components}
    >
      {text}
    </ReactMarkdown>
  );
};

const renderContent = (result: unknown) => {
  if (!result) {
    return <Text style={styles.text}>No result available</Text>;
  }

  if (typeof result === 'string') {
    return renderMarkdown(result);
  }

  if (typeof result === 'object' && result !== null) {
    if ('data' in result && result.data) {
      const data = result.data;
      if (typeof data === 'string') {
        return renderMarkdown(data);
      }
      if (typeof data === 'object' && data !== null) {
        return Object.entries(data).map(([key, value]) => (
          <View key={key} style={styles.section}>
            <Text style={styles.sectionTitle}>{key}</Text>
            {typeof value === 'string' ? (
              renderMarkdown(value)
            ) : (
              <Text style={styles.codeBlock}>
                {JSON.stringify(value, null, 2)}
              </Text>
            )}
          </View>
        ));
      }
    }
    return Object.entries(result).map(([key, value]) => (
      <View key={key} style={styles.section}>
        <Text style={styles.sectionTitle}>{key}</Text>
        {typeof value === 'string' ? (
          renderMarkdown(value)
        ) : (
          <Text style={styles.codeBlock}>
            {JSON.stringify(value, null, 2)}
          </Text>
        )}
      </View>
    ));
  }

  return <Text style={styles.text}>{String(result)}</Text>;
};

const PDFDocument: React.FC<PDFDocumentProps> = ({ run }) => {
  return (
    <Document>
      <Page size="A4" style={styles.page}>
        <View style={styles.header}>
          <Text style={styles.title}>Run Result - {run.run_name}</Text>
          <Text style={styles.subtitle}>Status: {run.status}</Text>
          <Text style={styles.subtitle}>
            Created: {new Date(run.created_at).toLocaleString()}
          </Text>
        </View>
        <View style={styles.section}>
          {renderContent(run.result)}
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
    link.download = `${sanitizedName}-${run.job_id}.pdf`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  } catch (error) {
    console.error('Error generating PDF:', error);
    throw error;
  }
}; 