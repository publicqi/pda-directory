declare module 'base58-js' {
  /** Convert a base58 encoded string to its binary representation. */
  export function base58_to_binary(base58String: string): Uint8Array;

  /** Convert raw bytes into a base58 encoded string. */
  export function binary_to_base58(input: ArrayLike<number>): string;
}
