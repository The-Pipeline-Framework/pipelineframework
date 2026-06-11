import "./globals.css";

export const metadata = {
  title: "TPFGo Checkout Flow Explorer",
  description: "Guided service-flow explorer for the TPFGo checkout checkpoint example."
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
