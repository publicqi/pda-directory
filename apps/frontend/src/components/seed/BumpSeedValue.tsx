import TogglableSeedValue from './TogglableSeedValue';

interface BumpSeedValueProps {
    bumpValue: number;
    rawHex: string;
}

const BumpSeedValue = ({ bumpValue, rawHex }: BumpSeedValueProps) => {
    return (
        <TogglableSeedValue
            valueA={String(bumpValue)}
            valueB={rawHex}
            label="Bump"
            badgeClass="badge-accent"
        />
    );
};

export default BumpSeedValue;
