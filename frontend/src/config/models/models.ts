import { Models } from '../../types/models';

/**
 * @deprecated This file is being replaced by the database-backed model configuration system.
 * 
 * Default models have been disabled to prevent conflicts with the database.
 * New code should use ModelService instead of importing from this file directly:
 * 
 * ```
 * import { ModelService } from '../api/ModelService';
 * 
 * // In an async function:
 * const modelService = ModelService.getInstance();
 * const models = await modelService.getActiveModels();
 * ```
 */

// All default models have been removed to prevent fallback conflicts with the database
export const models: Models = {}; 