// src/components/Register.jsx
import React, { useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import { UserPlus, User, LockKeyhole, Mail, AlertCircle } from 'lucide-react';
import useAuthStore from '../stores/authStore';
import toast from 'react-hot-toast';

const Register = () => {
  const navigate = useNavigate();
  const { register, isLoading, error, clearError } = useAuthStore();
  const [formData, setFormData] = useState({
    username: '',
    password: '',
    confirmPassword: '',
    email: ''
  });
  const [validationErrors, setValidationErrors] = useState({});

  const validateForm = () => {
    const errors = {};

    if (formData.username.length < 3) {
      errors.username = 'İstifadəçi adı ən azı 3 simvol olmalıdır';
    }

    if (formData.password.length < 6) {
      errors.password = 'Şifrə ən azı 6 simvol olmalıdır';
    }

    if (formData.password !== formData.confirmPassword) {
      errors.confirmPassword = 'Şifrələr uyğun gəlmir';
    }

    if (formData.email && !/^[\w\.-]+@[\w\.-]+\.\w+$/.test(formData.email)) {
      errors.email = 'Email formatı yanlışdır';
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!validateForm()) return;

    const result = await register(
      formData.username,
      formData.password,
      formData.email
    );

    if (result.success) {
      toast.success('Qeydiyyat uğurlu!');
      navigate('/dashboard');
    } else {
      toast.error(typeof result.error === 'string' ? result.error : 'Qeydiyyat xətası');
    }
  };

  const handleChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value
    });
    clearError();
    setValidationErrors({ ...validationErrors, [e.target.name]: '' });
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-purple-900 via-blue-900 to-indigo-900 flex items-center justify-center px-4">
      <div className="max-w-md w-full">
        <div className="bg-white/10 backdrop-blur-lg rounded-2xl p-8 border border-white/20">
          <div className="text-center mb-8">
            <div className="bg-white/20 rounded-full w-16 h-16 flex items-center justify-center mx-auto mb-4">
              <UserPlus className="w-8 h-8 text-white" />
            </div>
            <h2 className="text-2xl font-bold text-white mb-2">Qeydiyyat</h2>
            <p className="text-blue-200">Yeni hesab yaradın</p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-blue-100 mb-2">
                İstifadəçi adı
              </label>
              <div className="relative">
                <User className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-blue-300" />
                <input
                  type="text"
                  name="username"
                  value={formData.username}
                  onChange={handleChange}
                  className="w-full pl-10 pr-4 py-3 bg-white/10 border border-white/30 rounded-lg text-white placeholder-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  placeholder="İstifadəçi adınız"
                  required
                />
              </div>
              {validationErrors.username && (
                <p className="text-red-300 text-xs mt-1">{validationErrors.username}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-medium text-blue-100 mb-2">
                Email (istəyə bağlı)
              </label>
              <div className="relative">
                <Mail className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-blue-300" />
                <input
                  type="email"
                  name="email"
                  value={formData.email}
                  onChange={handleChange}
                  className="w-full pl-10 pr-4 py-3 bg-white/10 border border-white/30 rounded-lg text-white placeholder-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  placeholder="email@example.com"
                />
              </div>
              {validationErrors.email && (
                <p className="text-red-300 text-xs mt-1">{validationErrors.email}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-medium text-blue-100 mb-2">
                Şifrə
              </label>
              <div className="relative">
                <LockKeyhole className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-blue-300" />
                <input
                  type="password"
                  name="password"
                  value={formData.password}
                  onChange={handleChange}
                  className="w-full pl-10 pr-4 py-3 bg-white/10 border border-white/30 rounded-lg text-white placeholder-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  placeholder="Ən azı 6 simvol"
                  required
                />
              </div>
              {validationErrors.password && (
                <p className="text-red-300 text-xs mt-1">{validationErrors.password}</p>
              )}
            </div>

            <div>
              <label className="block text-sm font-medium text-blue-100 mb-2">
                Şifrəni təsdiqlə
              </label>
              <div className="relative">
                <LockKeyhole className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-blue-300" />
                <input
                  type="password"
                  name="confirmPassword"
                  value={formData.confirmPassword}
                  onChange={handleChange}
                  className="w-full pl-10 pr-4 py-3 bg-white/10 border border-white/30 rounded-lg text-white placeholder-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-400"
                  placeholder="Şifrəni təkrar yazın"
                  required
                />
              </div>
              {validationErrors.confirmPassword && (
                <p className="text-red-300 text-xs mt-1">{validationErrors.confirmPassword}</p>
              )}
            </div>

            {error && (
              <div className="bg-red-500/20 border border-red-500/50 rounded-lg p-3 flex items-center space-x-2">
                <AlertCircle className="w-5 h-5 text-red-300 flex-shrink-0" />
                <p className="text-red-200 text-sm">{typeof error === 'string' ? error : (error?.message || JSON.stringify(error))}</p>
              </div>
            )}

            <button
              type="submit"
              disabled={isLoading}
              className="w-full bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700 text-white font-semibold py-3 rounded-lg transition-all transform hover:scale-105 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? 'Qeydiyyat edilir...' : 'Qeydiyyatdan keç'}
            </button>
          </form>

          <div className="mt-6 text-center">
            <p className="text-blue-200 text-sm">
              Artıq hesabınız var?{' '}
              <Link to="/login" className="text-white hover:underline font-semibold">
                Giriş edin
              </Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Register;
