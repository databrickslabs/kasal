import { describe, expect, it } from 'vitest';
import { hasSkillBlock, parseSkillMarkdown, splitSkillSegments } from './skillBlock';

const MD = '---\nname: writing-release-notes\ndescription: "Use when: drafting notes. Trigger when the user mentions releases."\n---\n\n# Writing release notes\n\n## When to use this skill\nAny release.\n';

describe('splitSkillSegments', () => {
  it('leaves content without a skill fence as one text segment', () => {
    const segs = splitSkillSegments('plain **markdown**');
    expect(segs).toEqual([{ type: 'text', text: 'plain **markdown**' }]);
    expect(hasSkillBlock(segs)).toBe(false);
  });

  it('splits a closed ```skill block with text around it', () => {
    const segs = splitSkillSegments(`Here is the draft:\n\n\`\`\`skill\n${MD}\`\`\`\n\nI left out X.`);
    expect(segs.map((s) => s.type)).toEqual(['text', 'skill', 'text']);
    const skill = segs[1] as { type: 'skill'; code: string; closed: boolean };
    expect(skill.closed).toBe(true);
    expect(skill.code.startsWith('---\nname: writing-release-notes')).toBe(true);
  });

  it('returns an UNCLOSED trailing block while the draft streams', () => {
    const segs = splitSkillSegments('Drafting…\n```skill\n---\nname: partial');
    expect(segs[1]).toEqual({ type: 'skill', code: '---\nname: partial', closed: false });
  });

  it('ignores other fence languages', () => {
    const segs = splitSkillSegments('```python\nprint(1)\n```');
    expect(hasSkillBlock(segs)).toBe(false);
  });
});

describe('parseSkillMarkdown', () => {
  it('reads name/description from the front-matter and the rest as body', () => {
    const d = parseSkillMarkdown(MD);
    expect(d.name).toBe('writing-release-notes');
    expect(d.description).toBe('Use when: drafting notes. Trigger when the user mentions releases.');
    expect(d.body.startsWith('# Writing release notes')).toBe(true);
    expect(d.license).toBeNull();
  });

  it('is tolerant of a draft with no front-matter yet', () => {
    const d = parseSkillMarkdown('---\nname: partial');
    expect(d.name).toBe('');
    expect(d.body).toBe('---\nname: partial');
  });
});
