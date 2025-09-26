import PubkeyDisplay from '../PubkeyDisplay';

interface PubkeySeedValueProps {
    base58Value: string;
}

const PubkeySeedValue = ({ base58Value }: PubkeySeedValueProps) => {
    return (
        <div className="seed-value-layout">
            <div className="seed-tag">
                <span className="badge badge-accent">Pubkey</span>
            </div>
            <PubkeyDisplay pubkey={base58Value} />
        </div>
    );
};

export default PubkeySeedValue;
