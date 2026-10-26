import { useState } from 'react';
import { Accordion, AccordionDetails, AccordionSummary, Typography } from '@mui/material';
import { ChevronDown } from 'lucide-react';
import SchemaFieldsEditor from '../../../configuration/schemas/components/SchemaFieldsEditor';
import { fieldsToSchema, schemaToFields } from '../../../../utils/schemaModel';

export interface FlowOutputContract {
  crew_id: string;
  task_id: string;
  name: string;
  schema_definition: Record<string, unknown>;
}

/** Reuse the schema editor while keeping this output contract local to its flow. */
export default function FlowOutputSchema({ contract, onChange }: {
  contract: FlowOutputContract; onChange: (contract: FlowOutputContract) => void;
}) {
  const [fields, setFields] = useState(() => schemaToFields(contract.schema_definition));
  return <Accordion disableGutters elevation={0} sx={{ bgcolor: 'transparent', '&:before': { display: 'none' }, mb: 1 }}>
    <AccordionSummary expandIcon={<ChevronDown size={16} />} sx={{ px: 0 }}>
      <Typography variant="body2">Review output fields</Typography>
    </AccordionSummary>
    <AccordionDetails sx={{ px: 0 }}>
      <Typography variant="caption" color="text.secondary" component="p" sx={{ mb: 1.5 }}>
        Applied to the source crew’s final task in this flow. Other uses of the catalog crew keep their original output.
      </Typography>
      <SchemaFieldsEditor fields={fields} showRequired onChange={next => {
        setFields(next);
        onChange({ ...contract, schema_definition: fieldsToSchema(next) });
      }} />
    </AccordionDetails>
  </Accordion>;
}
