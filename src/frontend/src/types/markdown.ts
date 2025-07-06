import React from 'react';

export type MarkdownProps = {
  children?: React.ReactNode;
  className?: string;
} & Record<string, unknown>; 