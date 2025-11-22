export default function AboutPage() {
  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-4xl mx-auto text-center">
        {/* Hero Section */}
        <h1 className="text-4xl font-bold text-pink-600 mb-4">About KidFlix</h1>
        <p className="text-gray-700 text-lg mb-12">
          Making Learning Fun for Every Child
        </p>

        {/* Mission */}
        <div className="bg-pink-50 p-8 rounded-xl shadow-sm mb-12">
          <h2 className="text-2xl font-semibold text-pink-600 mb-3">Our Mission</h2>
          <p className="text-gray-700">
            At KidFlix, we believe in combining entertainment with education.
            Our mission is to provide safe, age-appropriate, and fun content
            that helps kids learn and grow in a secure online space.
          </p>
        </div> 

        {/* Our Story */}
        <div className="text-left mb-12">
          <h2 className="text-2xl font-semibold text-pink-600 mb-3">Our Story</h2>
          <p className="text-gray-700 mb-4">
            KidFlix was born out of a simple idea — to create a safe and engaging
            space where children can explore books and videos that inspire them.
            As parents and educators, we wanted a platform that kids would love
            while giving parents peace of mind.
          </p>
          <p className="text-gray-700">
            Today, KidFlix helps children around the world discover new interests
            through personalized recommendations and interactive learning.
          </p>
        </div>

        {/* What Makes Us Special */}
        <div className="grid md:grid-cols-2 gap-6 mb-12">
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="font-semibold text-pink-600 mb-2">Personalized Content</h3>
            <p className="text-gray-700">Recommendations tailored to each child’s age and interests.</p>
          </div>
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="font-semibold text-pink-600 mb-2">Safe Environment</h3>
            <p className="text-gray-700">Every book and video is reviewed for safety and appropriateness.</p>
          </div>
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="font-semibold text-pink-600 mb-2">Interactive Chatbot</h3>
            <p className="text-gray-700">A friendly assistant helps kids find what they’re looking for.</p>
          </div>
          <div className="bg-white p-6 rounded-lg shadow-md border border-gray-200">
            <h3 className="font-semibold text-pink-600 mb-2">Parental Controls</h3>
            <p className="text-gray-700">Tools for parents to monitor and guide their child’s experience.</p>
          </div>
        </div>

        {/* Call to Action */}
        <div className="bg-pink-100 p-8 rounded-xl shadow-md">
          <h2 className="text-2xl font-bold text-pink-600 mb-3">Join KidFlix Today</h2>
          <p className="text-gray-700 mb-6">
            Create a free account and start exploring a world of fun and learning for your kids.
          </p>
          <a
            href="/register"
            className="bg-pink-600 text-white font-semibold px-6 py-2 rounded-md hover:bg-pink-700 transition"
          >
            Get Started
          </a>
        </div>
      </div>
    </section>
  );
}
