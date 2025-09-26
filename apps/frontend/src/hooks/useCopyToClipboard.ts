import { useState, useCallback, useRef, useEffect } from 'react';

const useCopyToClipboard = (text: string, timeout = 2000) => {
    const [copied, setCopied] = useState(false);
    const timeoutId = useRef<number | undefined>();

    const copy = useCallback(() => {
        navigator.clipboard.writeText(text).then(
            () => {
                setCopied(true);
                clearTimeout(timeoutId.current);
                timeoutId.current = setTimeout(() => setCopied(false), timeout);
            },
            (err) => {
                console.error('Failed to copy: ', err);
            },
        );
    }, [text, timeout]);

    useEffect(() => {
        return () => {
            clearTimeout(timeoutId.current);
        };
    }, []);

    return { copied, copy };
};

export default useCopyToClipboard;
