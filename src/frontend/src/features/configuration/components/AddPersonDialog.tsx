import React, { useState } from 'react';
import { Alert, Box, Button, Dialog, DialogActions, DialogContent, DialogTitle, List, ListItem, ListItemText, TextField, Typography } from '@mui/material';
import { DirectoryPerson, User, UserService } from '../../../api/groups/UserService';

export default function AddPersonDialog({ onClose, onAdded }: {
  onClose: () => void;
  onAdded: (user: User) => void;
}) {
  const [email, setEmail] = useState('');
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<DirectoryPerson[]>([]);
  const [searched, setSearched] = useState(false);
  const [searching, setSearching] = useState(false);
  const [adding, setAdding] = useState(false);
  const [error, setError] = useState('');

  const search = async (event: React.FormEvent) => {
    event.preventDefault();
    setSearching(true);
    setError('');
    setResults([]);
    setSearched(false);
    try {
      setResults(await UserService.getInstance().searchDirectory(query.trim()));
      setSearched(true);
    } catch (error) {
      const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
      setError(typeof detail === 'string' && detail.trim()
        ? detail
        : 'Could not contact the directory search service. Please try again, or add the person by their exact sign-in email.');
    } finally {
      setSearching(false);
    }
  };

  const add = async (event: React.FormEvent) => {
    event.preventDefault();
    setAdding(true);
    setError('');
    try {
      onAdded(await UserService.getInstance().provisionUser(email.trim()));
    } catch {
      setError('Could not add this person. Check the sign-in email and try again.');
    } finally {
      setAdding(false);
    }
  };

  return (
    <Dialog open onClose={adding ? undefined : onClose} fullWidth maxWidth="sm">
      <DialogTitle>Add person</DialogTitle>
      <DialogContent>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Add someone before their first visit, then set their permissions in People.
          Use the email they sign in with.
        </Typography>
        {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
        <Box component="form" onSubmit={search} sx={{ display: 'flex', gap: 1, mb: 1 }}>
          <TextField label="Search Databricks directory" value={query} onChange={(e) => setQuery(e.target.value)}
            size="small" fullWidth disabled={searching || adding} inputProps={{ maxLength: 200 }} />
          <Button type="submit" disabled={searching || adding || query.trim().length < 2}>
            {searching ? 'Searching…' : 'Search'}
          </Button>
        </Box>
        {searched && results.length === 0 && <Typography variant="body2" sx={{ mb: 2 }}>No matching people. You can enter their sign-in email below.</Typography>}
        <List dense>
          {results.map((person) => (
            <ListItem key={person.email} secondaryAction={
              <Button disabled={adding} onClick={() => setEmail(person.email)} aria-label={`Select ${person.email}`}>Select</Button>
            }>
              <ListItemText primary={person.email} secondary={person.display_name} />
            </ListItem>
          ))}
        </List>
        <Box component="form" id="add-person-form" onSubmit={add}>
          <TextField label="Sign-in email" type="email" required fullWidth size="small" value={email}
            disabled={adding} onChange={(e) => setEmail(e.target.value)} />
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={adding}>Cancel</Button>
        <Button type="submit" form="add-person-form" variant="contained" disabled={adding || !email.trim()}>
          {adding ? 'Adding…' : 'Add person'}
        </Button>
      </DialogActions>
    </Dialog>
  );
}
