/**
 * Power BI OAuth Hook
 *
 * Handles Microsoft OAuth flow for Power BI API access.
 * Allows users to authenticate with their own Microsoft account
 * instead of using a Service Principal.
 */

import { useState, useCallback, useEffect } from 'react';

// Power BI API scope
const POWERBI_SCOPE = 'https://analysis.windows.net/powerbi/api/.default';
const FABRIC_SCOPE = 'https://api.fabric.microsoft.com/.default';

interface PowerBIOAuthConfig {
  clientId: string;
  tenantId?: string; // If not provided, uses 'common' for multi-tenant
  redirectUri?: string;
  scopes?: string[];
}

interface PowerBIOAuthState {
  accessToken: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
  userEmail: string | null;
  expiresAt: Date | null;
}

interface UsePowerBIOAuthReturn extends PowerBIOAuthState {
  signIn: () => void;
  signOut: () => void;
  handleCallback: (code: string) => Promise<void>;
}

// Storage keys
const STORAGE_KEY_TOKEN = 'powerbi_oauth_token';
const STORAGE_KEY_EXPIRES = 'powerbi_oauth_expires';
const STORAGE_KEY_EMAIL = 'powerbi_oauth_email';

/**
 * Hook for Power BI OAuth authentication
 *
 * Usage:
 * ```tsx
 * const { accessToken, isAuthenticated, signIn, signOut, error } = usePowerBIOAuth({
 *   clientId: 'your-azure-app-client-id',
 *   redirectUri: window.location.origin + '/powerbi/callback',
 * });
 *
 * // In your component:
 * {!isAuthenticated ? (
 *   <Button onClick={signIn}>Sign in with Microsoft</Button>
 * ) : (
 *   <Button onClick={signOut}>Sign out</Button>
 * )}
 *
 * // Pass accessToken to your API calls
 * ```
 */
export function usePowerBIOAuth(config?: PowerBIOAuthConfig): UsePowerBIOAuthReturn {
  const [state, setState] = useState<PowerBIOAuthState>({
    accessToken: null,
    isAuthenticated: false,
    isLoading: false,
    error: null,
    userEmail: null,
    expiresAt: null,
  });

  // Default config - uses a common multi-tenant app registration
  // In production, this should be configured per deployment
  const effectiveConfig: PowerBIOAuthConfig = {
    clientId: config?.clientId || '',
    tenantId: config?.tenantId || 'common',
    redirectUri: config?.redirectUri || `${window.location.origin}/powerbi/callback`,
    scopes: config?.scopes || [POWERBI_SCOPE, FABRIC_SCOPE, 'offline_access', 'openid', 'profile', 'email'],
  };

  // Check for existing token on mount
  useEffect(() => {
    const storedToken = sessionStorage.getItem(STORAGE_KEY_TOKEN);
    const storedExpires = sessionStorage.getItem(STORAGE_KEY_EXPIRES);
    const storedEmail = sessionStorage.getItem(STORAGE_KEY_EMAIL);

    if (storedToken && storedExpires) {
      const expiresAt = new Date(storedExpires);
      if (expiresAt > new Date()) {
        setState({
          accessToken: storedToken,
          isAuthenticated: true,
          isLoading: false,
          error: null,
          userEmail: storedEmail,
          expiresAt,
        });
      } else {
        // Token expired, clear storage
        sessionStorage.removeItem(STORAGE_KEY_TOKEN);
        sessionStorage.removeItem(STORAGE_KEY_EXPIRES);
        sessionStorage.removeItem(STORAGE_KEY_EMAIL);
      }
    }
  }, []);

  /**
   * Initiate OAuth sign-in flow
   * Opens Microsoft login page in the same window
   */
  const signIn = useCallback(() => {
    if (!effectiveConfig.clientId) {
      setState(prev => ({
        ...prev,
        error: 'Power BI OAuth is not configured. Please provide a Client ID.',
      }));
      return;
    }

    setState(prev => ({ ...prev, isLoading: true, error: null }));

    // Store current URL to redirect back after auth
    sessionStorage.setItem('powerbi_oauth_return_url', window.location.href);

    // Build authorization URL
    const authUrl = new URL(`https://login.microsoftonline.com/${effectiveConfig.tenantId}/oauth2/v2.0/authorize`);
    authUrl.searchParams.set('client_id', effectiveConfig.clientId);
    authUrl.searchParams.set('response_type', 'code');
    authUrl.searchParams.set('redirect_uri', effectiveConfig.redirectUri!);
    authUrl.searchParams.set('scope', effectiveConfig.scopes!.join(' '));
    authUrl.searchParams.set('response_mode', 'query');
    authUrl.searchParams.set('prompt', 'select_account');

    // Generate and store state for CSRF protection
    const oauthState = crypto.randomUUID();
    sessionStorage.setItem('powerbi_oauth_state', oauthState);
    authUrl.searchParams.set('state', oauthState);

    // Redirect to Microsoft login
    window.location.href = authUrl.toString();
  }, [effectiveConfig]);

  /**
   * Handle OAuth callback after user authenticates
   */
  const handleCallback = useCallback(async (code: string) => {
    if (!effectiveConfig.clientId) {
      setState(prev => ({
        ...prev,
        error: 'Power BI OAuth is not configured.',
        isLoading: false,
      }));
      return;
    }

    setState(prev => ({ ...prev, isLoading: true, error: null }));

    try {
      // Exchange code for tokens
      // Note: In production, this should be done via your backend to protect the client secret
      // For public clients (SPA), use PKCE flow instead
      const tokenUrl = `https://login.microsoftonline.com/${effectiveConfig.tenantId}/oauth2/v2.0/token`;

      const params = new URLSearchParams();
      params.append('client_id', effectiveConfig.clientId);
      params.append('grant_type', 'authorization_code');
      params.append('code', code);
      params.append('redirect_uri', effectiveConfig.redirectUri!);
      params.append('scope', effectiveConfig.scopes!.join(' '));

      const response = await fetch(tokenUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        body: params.toString(),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error_description || 'Token exchange failed');
      }

      const tokenData = await response.json();
      const accessToken = tokenData.access_token;
      const expiresIn = tokenData.expires_in || 3600;
      const expiresAt = new Date(Date.now() + expiresIn * 1000);

      // Decode ID token to get user email (if available)
      let userEmail = null;
      if (tokenData.id_token) {
        try {
          const payload = JSON.parse(atob(tokenData.id_token.split('.')[1]));
          userEmail = payload.email || payload.preferred_username || null;
        } catch {
          // Ignore ID token parsing errors
        }
      }

      // Store in session storage
      sessionStorage.setItem(STORAGE_KEY_TOKEN, accessToken);
      sessionStorage.setItem(STORAGE_KEY_EXPIRES, expiresAt.toISOString());
      if (userEmail) {
        sessionStorage.setItem(STORAGE_KEY_EMAIL, userEmail);
      }

      setState({
        accessToken,
        isAuthenticated: true,
        isLoading: false,
        error: null,
        userEmail,
        expiresAt,
      });

      // Redirect back to original page
      const returnUrl = sessionStorage.getItem('powerbi_oauth_return_url');
      if (returnUrl) {
        sessionStorage.removeItem('powerbi_oauth_return_url');
        window.location.href = returnUrl;
      }
    } catch (err: any) {
      setState(prev => ({
        ...prev,
        isLoading: false,
        error: err.message || 'Failed to authenticate with Microsoft',
      }));
    }
  }, [effectiveConfig]);

  /**
   * Sign out - clear stored tokens
   */
  const signOut = useCallback(() => {
    sessionStorage.removeItem(STORAGE_KEY_TOKEN);
    sessionStorage.removeItem(STORAGE_KEY_EXPIRES);
    sessionStorage.removeItem(STORAGE_KEY_EMAIL);
    sessionStorage.removeItem('powerbi_oauth_state');
    sessionStorage.removeItem('powerbi_oauth_return_url');

    setState({
      accessToken: null,
      isAuthenticated: false,
      isLoading: false,
      error: null,
      userEmail: null,
      expiresAt: null,
    });
  }, []);

  return {
    ...state,
    signIn,
    signOut,
    handleCallback,
  };
}

/**
 * Check if URL contains OAuth callback parameters
 */
export function isPowerBIOAuthCallback(): boolean {
  const params = new URLSearchParams(window.location.search);
  return params.has('code') && params.has('state');
}

/**
 * Get OAuth code from callback URL
 */
export function getPowerBIOAuthCode(): { code: string | null; state: string | null; error: string | null } {
  const params = new URLSearchParams(window.location.search);
  return {
    code: params.get('code'),
    state: params.get('state'),
    error: params.get('error_description') || params.get('error'),
  };
}

export default usePowerBIOAuth;
